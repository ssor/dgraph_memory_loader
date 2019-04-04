package main

import (
	"github.com/mkideal/cli"
	"github.com/ssor/dgraph_live_client"
	"io/ioutil"
	"os"
)

type LiveArgs struct {
	cli.Helper
	DataFile            string `cli:"f,file" usage:"Location of *.rdf(.gz) or *.json(.gz) file to load"`
	DataFormat          string `cli:"format" usage:"Specify file format (rdf or json) instead of getting it from filename"`
	SchemaFile          string `cli:"s,schema" usage:"Location of schema file"`
	Dgraph              string `cli:"d,dgraph" usage:"Dgraph alpha gRPC server address" dft:"127.0.0.1:9080"`
	Zero                string `cli:"z,zero" usage:"Dgraph zero gRPC server address" dft:"127.0.0.1:5080"`
	ClientDir           string `cli:"X,xidmap" usage:"Directory to store xid to uid mapping" dft:""`
	Concurrent          int    `cli:"c,conc" usage:"Number of concurrent requests to make to Dgraph" dft:"10"`
	BatchSize           int    `cli:"b,batch" usage:"Number of N-Quads to send as part of a mutation." dft:"1000"`
	IgnoreIndexConflict bool   `cli:"i,ignore_index_conflict" usage:"Ignores conflicts on index keys during transaction" dft:"true"`
	AuthToken           string `cli:"a,auth_token" usage:"The auth token passed to the server for Alter operation of the schema file" dft:""`
	UseCompression      bool   `cli:"C,use_compression" usage:"Enable compression on connection to alpha server" dft:"false"`
	NewUids             bool   `cli:"new_uids" usage:"Ignore UIDs in load files and assign new ones." dft:"false"`
	TlsServerName       string `cli:"tls_server_name" usage:"Used to verify the server hostname" dft:""`
	TlsCACert           string `cli:"tls_cacert" usage:"" dft:""`
	TlsUseSystemCA      bool   `cli:"tls_use_system_ca" usage:"" dft:"false"`
	TlsCert             string `cli:"tls_cert" usage:"" dft:""`
	TlsKey              string `cli:"tls_key" usage:"" dft:""`
}

func main() {
	os.Exit(cli.Run(new(LiveArgs), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*LiveArgs)
		return run(argv)
	}))
}

func run(argv *LiveArgs) error {
	bs, err := ioutil.ReadFile(argv.DataFile)
	if err != nil {
		return err
	}
	ops := dgraph_live_client.NewBatchMutaionOptions(argv.BatchSize, argv.Concurrent)
	loader := dgraph_live_client.NewMemoryLoader(argv.Zero, argv.Dgraph, ops)
	return loader.Load(bs)
}
