package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Releases struct {
	Version string
}

type Repository struct {
	URL      string `json:"url"`
	Releases []Releases
}

type Achievement struct {
	Name        string
	AwardedDate time.Time
}
type Account struct {
	Id            uint64
	Name          string
	Organizations []string `json:"orgs"`
	Repositories  []Repository
	Achievement   Achievement
}

type GithubEvent struct {
	Title        string
	Type         string
	Assignee     Account  `json:"assignee"`
	Labels       []string `json:"labels"`
	Contributors []Account
}

type InconsistentAccount struct {
	Id            string
	Name          string
	Organizations []string `json:"orgs"`
	Repositories  []Repository
	Achievement   Achievement
}

type InconsistentGithubEvent struct {
	Title        string
	EventType    string
	Assignee     InconsistentAccount `json:"assignee"`
	Labels       []string            `json:"labels"`
	Contributors []InconsistentAccount
}

func TestJSON(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			DialTimeout: time.Hour,
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"allow_experimental_object_type": 1,
			},
		})
	)
	conn.Exec(ctx, "DROP TABLE json_test")
	ddl := `CREATE table json_test(event JSON) ENGINE=Memory;`
	if assert.NoError(t, err) {
		defer func() {
			conn.Exec(ctx, "DROP TABLE json_test")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test"); assert.NoError(t, err) {
				col1Data := GithubEvent{
					Title: "Document JSON support",
					Type:  "Issue",
					Assignee: Account{
						Id:            1244,
						Name:          "Geoff",
						Achievement:   Achievement{Name: "Mars Star", AwardedDate: time.Now().Truncate(time.Second)},
						Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
						Organizations: []string{"Support Engineer", "Integrations"},
					},
					Labels: []string{"Help wanted"},
					Contributors: []Account{
						{Id: 2244, Achievement: Achievement{Name: "Adding JSON to go driver", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"Support Engineer", "Consulting", "PM", "Integrations"}, Name: "Dale", Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}, {URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.2.0"}, {Version: "1.3.0"}}}}},
						{Id: 2344, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -700)}, Organizations: []string{"Support Engineer", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.4.0"}}}}},
					},
				}
				col2Data := GithubEvent{
					Title: "JSON support",
					Type:  "Pull Request",
					Assignee: Account{
						Id:            2244,
						Name:          "Dale",
						Achievement:   Achievement{Name: "Arctic Vault", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -1000)},
						Repositories:  []Repository{{URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.4.0"}, {Version: "1.6.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
						Organizations: []string{"Support Engineer", "Integrations"},
					},
					Labels: []string{"Bug"},
					Contributors: []Account{
						{Id: 1244, Name: "Geoff", Achievement: Achievement{Name: "Mars Star", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -3000)}, Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}}, Organizations: []string{"Support Engineer", "Integrations"}},
						{Id: 2244, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"ClickHouse", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.3.0"}}}}},
					},
				}

				assert.NoError(t, batch.Append(col1Data))
				assert.NoError(t, batch.Append(col2Data))
				if assert.NoError(t, batch.Send()) {
					var (
						col1 GithubEvent
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, col1Data, col1)
					}
				}
			}
		}
	}

}

func TestJSONImitate(t *testing.T) {

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			DialTimeout: time.Hour,
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"flatten_nested": 1,
			},
		})
	)
	conn.Exec(ctx, "DROP TABLE json_test")
	defer func() {
		conn.Exec(ctx, "DROP TABLE json_test")
	}()
	ddl := `CREATE table json_test(
				event Tuple(Title String, 
							Type String, 
							assignee Tuple(Id UInt64, Name String, orgs Array(String), Repositories Nested(url String, Releases Nested(Version String)), Achievement Tuple(Name String, AwardedDate String)), 
							labels Array(String), 
							Contributors Nested(Id UInt64, Name String, orgs Array(String), Repositories Nested(url String, Releases Nested(Version String)), Achievement Tuple(Name String, AwardedDate String)))
			) ENGINE=Memory;`
	if assert.NoError(t, err) {
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {

			sCol1 := GithubEvent{
				Title: "Document JSON support",
				Type:  "Issue",
				Assignee: Account{
					Id:            1244,
					Name:          "Geoff",
					Achievement:   Achievement{Name: "Mars Star", AwardedDate: time.Now().Truncate(time.Second)},
					Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
					Organizations: []string{"Support Engineer", "Integrations"},
				},
				Labels: []string{"Help wanted"},
				Contributors: []Account{
					{Id: 2244, Achievement: Achievement{Name: "Adding JSON to go driver", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"Support Engineer", "Consulting", "PM", "Integrations"}, Name: "Dale", Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}, {URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.2.0"}, {Version: "1.3.0"}}}}},
					{Id: 2344, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: time.Now().Truncate(time.Second).Add(time.Hour * -700)}, Organizations: []string{"Support Engineer", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.4.0"}}}}},
				},
			}
			col1Data := []interface{}{
				"Document JSON support",
				"Issue",
				[]interface{}{
					uint64(1244),
					"Geoff",
					[]string{"Support Engineer", "Integrations"},
					[][]interface{}{
						{"https://github.com/ClickHouse/clickhouse-python", [][]interface{}{{"1.0.0"}, {"1.1.0"}}},
						{"https://github.com/ClickHouse/clickhouse-go", [][]interface{}{{"2.0.0"}, {"2.1.0"}}},
					},
					[]interface{}{"Mars Star", time.Now().Truncate(time.Second).String()},
				},
				[]string{"Help wanted"},
				[][]interface{}{
					{
						uint64(2244),
						"Dale",
						[]string{"Support Engineer", "Consulting", "PM", "Integrations"},
						[][]interface{}{
							{"https://github.com/ClickHouse/clickhouse-go", [][]interface{}{{"2.0.0"}, {"2.1.0"}}},
							{"https://github.com/grafana/clickhouse", [][]interface{}{{"1.2.0"}, {"1.3.0"}}},
						},
						[]interface{}{"Adding JSON to go driver", time.Now().Truncate(time.Second).Add(time.Hour * -500).String()},
					},
					{
						uint64(2344),
						"Melyvn",
						[]string{"Support Engineer", "Consulting"},
						[][]interface{}{
							{"https://github.com/ClickHouse/support", [][]interface{}{{"1.0.0"}, {"2.3.0"}, {"2.4.0"}}},
						},
						[]interface{}{"Managing S3 buckets", time.Now().Truncate(time.Second).Add(time.Hour * -700).String()},
					},
				},
			}

			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test"); assert.NoError(t, err) {
				assert.NoError(t, batch.Append(col1Data))
				var (
					col1 GithubEvent
				)
				if assert.NoError(t, batch.Send()) {
					if err := conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, sCol1, col1)
					}
				}
			}
		}
	}
}

/*To test:

NEXT:
1. non exported type

1. Inconsistent types ( Non castable)
2. Castablae types
3. Inconsistent Structure
4. Missing field in struct
5. Multiple row read
6. Column format
7. Std interface
8. Stress test
10. test ip and uuid
11. Typed maps
12. Point
12. Big Int
13. Test point
14. Null types
*/
