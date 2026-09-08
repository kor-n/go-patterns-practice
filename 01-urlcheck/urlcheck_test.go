package urlcheck

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}

const handlerDelay = 80 * time.Millisecond

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(handlerDelay)
		if r.URL.Path == "/bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		srv.Close()
		http.DefaultClient.CloseIdleConnections()
	})
	return srv
}

func urls(srv *httptest.Server) []string {
	return []string{
		srv.URL + "/ok",
		srv.URL + "/bad",
		srv.URL + "/ok",
		srv.URL + "/bad",
		srv.URL + "/ok",
		srv.URL + "/ok",
	}
}

func assertResults(t *testing.T, name string, in []string, got []Result) {
	t.Helper()
	if len(got) != len(in) {
		t.Fatalf("%s: вернул %d результатов, ожидалось %d", name, len(got), len(in))
	}
	for i, u := range in {
		if got[i].URL != u {
			t.Errorf("%s: порядок нарушен, на позиции %d %q вместо %q", name, i, got[i].URL, u)
		}
		want := u[len(u)-3:] == "/ok"
		if got[i].OK != want {
			t.Errorf("%s: %s дал OK=%v, ожидалось %v", name, u, got[i].OK, want)
		}
	}
}

func TestCheckSeq(t *testing.T) {
	srv := newServer(t)
	in := urls(srv)
	assertResults(t, "CheckSeq", in, CheckSeq(in))
}

func TestCheckPar(t *testing.T) {
	srv := newServer(t)
	in := urls(srv)
	assertResults(t, "CheckPar", in, CheckPar(in))
}

func TestCheckParFasterThanSeq(t *testing.T) {
	srv := newServer(t)
	in := urls(srv)

	start := time.Now()
	CheckSeq(in)
	sequential := time.Since(start)

	start = time.Now()
	CheckPar(in)
	parallel := time.Since(start)

	if parallel > sequential/2 {
		t.Fatalf("CheckSeq занял %v, CheckPar %v: разницы почти нет", sequential, parallel)
	}
}

func TestCheckSeqReusesConnections(t *testing.T) {
	var opened atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	t.Cleanup(func() {
		srv.Close()
		http.DefaultClient.CloseIdleConnections()
	})

	in := make([]string, 6)
	for i := range in {
		in[i] = srv.URL + "/ok"
	}
	CheckSeq(in)

	if got := opened.Load(); got > 2 {
		t.Fatalf("на %d последовательных запросов открыто %d соединений: "+
			"тело ответа не закрывается, соединение не переиспользуется", len(in), got)
	}
}
