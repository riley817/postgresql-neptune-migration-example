package main

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	_ "github.com/lib/pq"
	"log"
	"os"
)

var (
	neptuneEndpoint = os.Getenv("NEPTUNE_ENDPOINT")
	host            = os.Getenv("PG_HOST")
	port            = os.Getenv("PG_PORT")
	user            = os.Getenv("PG_USER")
	dbname          = os.Getenv("PG_DBNAME")
	password        = os.Getenv("PG_PASSWORD")
	userLabel       = os.Getenv("USER_LABEL")
	followEdgeLabel = os.Getenv("FOLLOW_EDGE_LABEL") // follow 관계를 나타 내는 엣지 레이블
	f4fEdgeProp     = os.Getenv("FOLLOW_EDGE_NAME")  // follow 관계에서 맞팔로우 여부를 나타내는 엣지 속성
)

var (
	remoteConn *gremlingo.DriverRemoteConnection
	g          *gremlingo.GraphTraversalSource
	db         *sql.DB
	tx         *gremlingo.Transaction
)

type User struct {
	userId   string
	nickname sql.NullString
	birth    sql.NullString
}

var (
	GremlinErrNextNoResultsLeftError = "E0903: there are no results left"
	ErrDataNotFound                  = errors.New("data not found")
)

func init() {
	fmt.Println("init() called")
	// Creating the connection to the server
	var err error
	remoteConn, err = gremlingo.NewDriverRemoteConnection(fmt.Sprintf("wss://%s:8182/gremlin", neptuneEndpoint),
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TraversalSource = "g"
			settings.TlsConfig = &tls.Config{InsecureSkipVerify: true}
		})

	if err != nil {
		fmt.Printf("[Err] %+v", err)
		return
	}

	// Create an anonymous traversal source with remote
	g = gremlingo.Traversal_().WithRemote(remoteConn)

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s", host, port, user, dbname, password)
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Printf("[Err] %+v", err)
		remoteConn.Close()
	}
}

func main() {

	// Cleanup
	defer remoteConn.Close()
	defer db.Close()

	// 회원 수만 큼 vertex 생성
	rows, err := db.Query(`select user_id, nickname, to_char(birth, 'YYYY-MM-DD') as birth from users u where u.deleted_at is null`)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		u := &User{}
		if err := rows.Scan(&u.userId, &u.nickname, &u.birth); err != nil {
			panic(err)
		}

		if err := addUserVertex(u); err != nil {
			panic(err)
		}
	}

	// follow 테이블에서 edge 관계를 생성

	followRows, findFollowErr := db.Query(`select f.user_id as from_id, f.target_id as to_id from follow f where f.deleted_at is null order by f.created_at asc`)
	if findFollowErr != nil {
		panic(findFollowErr)
	}

	for followRows.Next() {
		var fromId, toId string
		if err := followRows.Scan(&fromId, &toId); err != nil {
			panic(err)
		}

		log.Printf("[addFollow] %s -> %s", fromId, toId)

		if err := addFollowEdge(fromId, toId); err != nil {
			panic(err)
		}
	}

}

func getUserVertex(vertexId string) (*gremlingo.Result, error) {
	res, err := g.V().HasLabel(userLabel).HasId(vertexId).Next()
	if err != nil {
		if err.Error() == GremlinErrNextNoResultsLeftError {
			return nil, ErrDataNotFound
		}
		return nil, err
	}
	return res, err
}

func existsUserVertex(vertexId string) (bool, error) {
	_, err := g.V().HasLabel(userLabel).HasId(vertexId).Next()
	if err != nil && err.Error() != GremlinErrNextNoResultsLeftError {
		return false, err
	}

	if err != nil && err.Error() == GremlinErrNextNoResultsLeftError {
		return false, nil
	}

	return true, nil
}

func addUserVertex(userInfo *User) error {

	exists, err := existsUserVertex(userInfo.userId)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	nickname, _ := userInfo.nickname.Value()
	birth, _ := userInfo.birth.Value()

	_, createdErr := g.AddV(userLabel).Property(gremlingo.T.Id, userInfo.userId).
		Property("nickname", nickname).
		Property("birth", birth).Next()

	if createdErr != nil {
		return createdErr
	}
	return nil
}

// findFollowEdge
func findFollowEdge(fromVertexId, toVertexId string, toObj interface{}) (string, error) {
	res, err := g.V().HasLabel(userLabel).HasId(fromVertexId).BothE(followEdgeLabel).Next()

	if err != nil && err.Error() != GremlinErrNextNoResultsLeftError {
		return "", err
	}

	if res == nil {
		return "", ErrDataNotFound
	}

	inV, _ := res.GetVertex()
	outV, _ := res.GetVertex()

	if inV.Id == toVertexId || outV.Id == toVertexId {

	} else {
		return "", ErrDataNotFound
	}

	edgeRes, err := res.GetEdge()
	if err != nil {
		return "", err
	}
	return edgeRes.Id.(string), err
}

func addFollowEdge(fromId, toId string) error {
	// 1. find from vertex
	fromObj, err := getUserVertex(fromId)
	if err != nil {
		log.Printf("[skip] skip %s", fromId)
		return nil
	}

	// 2. find to vertex
	toObj, err := getUserVertex(toId)
	if err != nil {
		log.Printf("[skip] skip %s", toId)
		return nil
	}

	// 3. follow edge 가 존재 하는지 체크
	edgeId, err := findFollowEdge(fromId, toId, toObj)
	if err != nil && !errors.Is(err, ErrDataNotFound) {
		return err
	}

	// 3.1 edge 가 없는 경우
	if errors.Is(err, ErrDataNotFound) {
		_, err = g.V(fromObj.Data).AddE(followEdgeLabel).From(fromObj.Data).To(toObj.Data).
			Property(f4fEdgeProp, false).Next()

		return nil
	}

	if err != nil {
		panic(err)
	}

	// 3.2 edge 가 있는 경우
	_, err = g.E().HasLabel(followEdgeLabel).HasId(edgeId).Property(f4fEdgeProp, true).Next()
	if err != nil {
		return err
	}
	return nil
}
