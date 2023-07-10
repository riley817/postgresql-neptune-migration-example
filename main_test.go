package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/stretchr/testify/suite"
	"testing"
)

type MainTestSuite struct {
	suite.Suite
	database *sql.DB
	conn     *gremlingo.DriverRemoteConnection
	graph    *gremlingo.GraphTraversalSource
}

func (ts *MainTestSuite) SetupTest() {
	// Creating the connection to the server
	var err error
	ts.conn, err = gremlingo.NewDriverRemoteConnection(fmt.Sprintf("wss://%s:8182/gremlin", neptuneEndpoint),
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TraversalSource = "g"
			settings.TlsConfig = &tls.Config{InsecureSkipVerify: true}
		})

	ts.NoError(err)

	// Create an anonymous traversal source with remote
	ts.graph = gremlingo.Traversal_().WithRemote(remoteConn)

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s", host, port, user, dbname, password)
	ts.database, err = sql.Open("postgres", psqlInfo)
	ts.NoError(err)
}

func TestNewMainTestsuite(t *testing.T) {
	suite.Run(t, new(MainTestSuite))
}

func (ts *MainTestSuite) TestGetVertex() {
	ts.Run("GetVertex", func() {
		defer ts.conn.Close()

		vertexId := "Lucy"
		result, err := ts.graph.V().HasLabel("User").HasId(vertexId).Next()
		ts.NoError(err)
		ts.NotEmpty(result)
		ts.T().Log(result.Data)
	})

	ts.Run("get not exists vertex", func() {
		defer ts.conn.Close()
		vertex := "yoon"
		result, err := ts.graph.V().HasLabel("User").HasId(vertex).Next()
		ts.Error(err)
		ts.Empty(result)
		ts.Equal(err.Error(), GremlinErrNextNoResultsLeftError)
	})
}

func (ts *MainTestSuite) TestAddVertex() {
	ts.Run("add vertex", func() {
		defer ts.conn.Close()

		res, err := ts.graph.AddV("test").Id(gremlingo.T.Id, "1111").Property("name", "test").Next()
		ts.NoError(err)
		ts.NotEmpty(res)

	})
}

func (ts *MainTestSuite) TestAddEdge() {
	ts.Run("add edge", func() {
		defer ts.conn.Close()

		fromVertexId := "10c302f2-acaf-0990-9a02-3785fa1ea5fc"
		toVertexId := "06c302e8-244e-7d7d-be10-c8c1f052e0ef"

		from, err := ts.graph.V().HasLabel("person").HasId(fromVertexId).Next()
		ts.NoError(err)
		ts.NotEmpty(from)

		to, err := ts.graph.V().HasLabel("person").HasId(toVertexId).Next()
		ts.NoError(err)
		ts.NotEmpty(to)

		// 이미 edge 존재 할 경우 X
		res, err := ts.graph.V(from.Data).OutE().HasLabel("follow").InV().HasId(toVertexId).Next()

		ts.NoError(err)
		if res != nil {
			return
		}

		res, err = ts.graph.V(from.Data).AddE("follow").From(from.Data).To(to.Data).Next()
		ts.NoError(err)
		ts.NotEmpty(res)
	})

	ts.Run("add property", func() {
		defer ts.conn.Close()
		edge, err := ts.graph.E().HasLabel("follow").HasId("30c302ee-de65-fbad-f054-f64df30ab4be").Next()
		ts.NoError(err)
		ts.NotEmpty(edge)

		res, err := ts.graph.E(edge.Data).Property("f4f", true).Next()
		ts.NoError(err)
		ts.NotEmpty(res)

	})
}

func (ts *MainTestSuite) TestExistsEdge() {
	ts.Run("해당 vertex 에 edge 가 존재 하는지 확인 한다.", func() {
		defer ts.conn.Close()

		fromVertexId := "Terry"
		toVertexId := "Mary"

		res, err := ts.graph.V().HasLabel("User").HasId(fromVertexId).BothE().Next()

		ts.T().Log()

		if err.Error() == "E0903: there are no results left" {
			ts.T().Log("edge not found")
			return
		}
		ts.Error(err)

		ts.T().Log(err.Error())

		edge, err := res.GetEdge()
		ts.NoError(err)
		ts.Equal(edge.InV.Id, toVertexId)
		ts.Equal(edge.OutV.Id, fromVertexId)
	})
}
