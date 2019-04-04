package dgraph_live_client

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/dgraph-io/dgo/y"
	"github.com/dgraph-io/dgraph/chunker"
	"github.com/dgraph-io/dgraph/x"
	"github.com/dgraph-io/dgraph/xidmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func NewMemoryLoader(zero, dgraph string, ops BatchMutationOptions) *MemoryLoader {
	loader := &MemoryLoader{
		BatchSize: ops.Size,
		reqs:      make(chan api.Mutation, 2*ops.Pending),
		opts:      ops,
	}

	var db *badger.DB
	connZero, err := x.SetupConnection(zero, nil, false)
	if err != nil {
		return nil
	}
	alloc := xidmap.New(connZero, db)
	loader.alloc = alloc
	loader.connZero = connZero
	loader.db = db

	ds := strings.Split(dgraph, ",")
	var clients []api.DgraphClient
	for _, d := range ds {
		conn, err := x.SetupConnection(d, nil, false)
		x.Checkf(err, "While trying to setup connection to Dgraph alpha %v", ds)

		dc := api.NewDgraphClient(conn)
		clients = append(clients, dc)
	}
	loader.dgraphClient = dgo.NewDgraphClient(clients...)

	return loader
}

type MemoryLoader struct {
	dgraphClient *dgo.Dgraph
	opts         BatchMutationOptions
	BatchSize    int
	reqs         chan api.Mutation
	alloc        *xidmap.XidMap
	connZero     *grpc.ClientConn
	requestsWg   sync.WaitGroup
	// If we retry a request, we add one to retryRequestsWg.
	retryRequestsWg sync.WaitGroup
	db              *badger.DB
	// Miscellaneous information to print counters.
	// Num of N-Quads sent
	nquads uint64
	// Num of txns sent
	txns uint64
	// Num of aborts
	aborts uint64
	// To get time elapsed
	start time.Time
}

func (loader *MemoryLoader) Load(raw []byte) error {
	rd := bufio.NewReader(bytes.NewBuffer(raw))
	ck := chunker.NewChunker(chunker.RdfFormat)

	loader.start = time.Now()

	loader.requestsWg.Add(loader.opts.Pending)

	for i := 0; i < loader.opts.Pending; i++ {
		go loader.makeRequests()
	}

	batch := make([]*api.NQuad, 0, 2*loader.BatchSize)
	for {
		select {
		case <-loader.opts.Ctx.Done():
			return loader.opts.Ctx.Err()
		default:
		}

		var nqs []*api.NQuad
		chunkBuf, err := ck.Chunk(rd)
		if chunkBuf != nil && chunkBuf.Len() > 0 {
			nqs, err = ck.Parse(chunkBuf)
			x.CheckfNoTrace(err)

			for _, nq := range nqs {
				nq.Subject = loader.uid(nq.Subject)
				if len(nq.ObjectId) > 0 {
					nq.ObjectId = loader.uid(nq.ObjectId)
				}
			}

			batch = append(batch, nqs...)
			for len(batch) >= loader.BatchSize {
				mu := api.Mutation{Set: batch[:loader.BatchSize]}
				loader.reqs <- mu
				// The following would create a new batch slice. We should not use batch =
				// batch[opt.BatchSize:], because it would end up modifying the batch array passed
				// to l.reqs above.
				batch = append([]*api.NQuad{}, batch[loader.BatchSize:]...)
			}
		}
		if err == io.EOF {
			if len(batch) > 0 {
				loader.reqs <- api.Mutation{Set: batch}
			}
			break
		} else {
			x.Check(err)
		}
	}
	x.CheckfNoTrace(ck.End(rd))

	close(loader.reqs)
	// First we wait for requestsWg, when it is done we know all retry requests have been added
	// to retryRequestsWg. We can't have the same waitgroup as by the time we call Wait, we can't
	// be sure that all retry requests have been added to the waitgroup.
	loader.requestsWg.Wait()
	loader.retryRequestsWg.Wait()

	c := loader.Counter()
	var rate uint64
	if c.Elapsed.Seconds() < 1 {
		rate = c.Nquads
	} else {
		rate = c.Nquads / uint64(c.Elapsed.Seconds())
	}
	// Lets print an empty line, otherwise Interrupted or Number of Mutations overwrites the
	// previous printed line.
	fmt.Printf("%100s\r", "")
	fmt.Printf("Number of TXs run            : %d\n", c.TxnsDone)
	fmt.Printf("Number of N-Quads processed  : %d\n", c.Nquads)
	fmt.Printf("Time spent                   : %v\n", c.Elapsed)
	fmt.Printf("N-Quads processed per second : %d\n", rate)

	if loader.db != nil {
		loader.alloc.Flush()
		loader.db.Close()
	}

	return nil
}

func (loader *MemoryLoader) uid(val string) string {
	// Attempt to parse as a UID (in the same format that dgraph outputs - a
	// hex number prefixed by "0x"). If parsing succeeds, then this is assumed
	// to be an existing node in the graph. There is limited protection against
	// a user selecting an unassigned UID in this way - it may be assigned
	// later to another node. It is up to the user to avoid this.
	if uid, err := strconv.ParseUint(val, 0, 64); err == nil {
		loader.alloc.BumpTo(uid)
		return fmt.Sprintf("%#x", uid)
	}

	uid := loader.alloc.AssignUid(val)
	return fmt.Sprintf("%#x", uint64(uid))
}

// makeRequests can receive requests from batchNquads or directly from BatchSetWithMark.
// It doesn't need to batch the requests anymore. Batching is already done for it by the
// caller functions.
func (loader *MemoryLoader) makeRequests() {
	defer loader.requestsWg.Done()
	for req := range loader.reqs {
		loader.request(req)
	}
}

func (loader *MemoryLoader) request(req api.Mutation) {
	txn := loader.dgraphClient.NewTxn()
	req.CommitNow = true
	_, err := txn.Mutate(loader.opts.Ctx, &req)

	if err == nil {
		atomic.AddUint64(&loader.nquads, uint64(len(req.Set)))
		atomic.AddUint64(&loader.txns, 1)
		return
	}
	handleError(err)
	atomic.AddUint64(&loader.aborts, 1)
	loader.retryRequestsWg.Add(1)
	go loader.infinitelyRetry(req)
}

func (loader *MemoryLoader) infinitelyRetry(req api.Mutation) {
	defer loader.retryRequestsWg.Done()
	for i := time.Millisecond; ; i *= 2 {
		txn := loader.dgraphClient.NewTxn()
		req.CommitNow = true
		_, err := txn.Mutate(loader.opts.Ctx, &req)
		if err == nil {
			atomic.AddUint64(&loader.nquads, uint64(len(req.Set)))
			atomic.AddUint64(&loader.txns, 1)
			return
		}
		handleError(err)
		atomic.AddUint64(&loader.aborts, 1)
		if i >= 10*time.Second {
			i = 10 * time.Second
		}
		time.Sleep(i)
	}
}

// Counter keeps a track of various parameters about a batch mutation. Running totals are printed
// if BatchMutationOptions PrintCounters is set to true.
type Counter struct {
	// Number of N-Quads processed by server.
	Nquads uint64
	// Number of mutations processed by the server.
	TxnsDone uint64
	// Number of Aborts
	Aborts uint64
	// Time elapsed since the batch started.
	Elapsed time.Duration
}

// Counter returns the current state of the BatchMutation.
func (loader *MemoryLoader) Counter() Counter {
	return Counter{
		Nquads:   atomic.LoadUint64(&loader.nquads),
		TxnsDone: atomic.LoadUint64(&loader.txns),
		Elapsed:  time.Since(loader.start),
		Aborts:   atomic.LoadUint64(&loader.aborts),
	}
}

// handleError inspects errors and terminates if the errors are non-recoverable.
// A gRPC code is Internal if there is an unforeseen issue that needs attention.
// A gRPC code is Unavailable when we can't possibly reach the remote server, most likely the
// server expects TLS and our certificate does not match or the host name is not verified. When
// the node certificate is created the name much match the request host name. e.g., localhost not
// 127.0.0.1.
func handleError(err error) {
	s := status.Convert(err)
	switch {
	case s.Code() == codes.Internal, s.Code() == codes.Unavailable:
		x.Fatalf(s.Message())
	case strings.Contains(s.Message(), "x509"):
		x.Fatalf(s.Message())
	case strings.Contains(s.Message(), "Server overloaded."):
		dur := time.Duration(1+rand.Intn(10)) * time.Minute
		fmt.Printf("Server is overloaded. Will retry after %s.", dur.Round(time.Minute))
		time.Sleep(dur)
	case err != y.ErrAborted && err != y.ErrConflict:
		fmt.Printf("Error while mutating: %v\n", s.Message())
	}
}
