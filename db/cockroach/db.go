package cockroach

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Properties
const (
	cockroachURL        = "cockroach.url"
	cockroachUser       = "cockroach.user"
	cockroachPassword   = "cockroach.password"
	cockroachDatabase   = "cockroach.database"
	cockroachSSLMode    = "cockroach.sslmode"
	cockroachTable      = "cockroach.table"
	cockroachDropSchema = "cockroach.drop_schema"
)

type cockroachCreator struct{}
type cockroachDB struct {
	p         *properties.Properties
	db        *pgxpool.Pool
	bufPool   *util.BufPool
	fieldKeys []string
	table     string
}

func (c cockroachCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	fmt.Println("here")
	d := new(cockroachDB)
	d.p = p
	d.bufPool = util.NewBufPool()

	connStr := p.GetString(cockroachURL, "postgresql://root@localhost:26257/test?sslmode=disable")
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.db = pool

	d.table = p.GetString(prop.TableName, prop.TableNameDefault)

	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	d.fieldKeys = make([]string, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		d.fieldKeys[i] = fmt.Sprintf("field%d", i)
	}

	if err := d.createTable(ctx); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *cockroachDB) createTable(ctx context.Context) error {
	if db.p.GetBool(prop.DropData, false) {
		_, _ = db.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", db.table))
	}

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (YCSB_KEY TEXT PRIMARY KEY", db.table))
	for _, f := range db.fieldKeys {
		buf.WriteString(fmt.Sprintf(", %s TEXT", f))
	}
	buf.WriteString(");")
	buf.WriteString(fmt.Sprintf(`
		ALTER TABLE %s CONFIGURE ZONE USING range_min_bytes=0, range_max_bytes = 1073741824;
		ALTER RANGE default CONFIGURE ZONE USING range_min_bytes=0, range_max_bytes = 1000000000;
	`, db.table))

	_, err := db.db.Exec(ctx, buf.String())
	return err
}

func (db *cockroachDB) Close() error {
	db.db.Close()
	return nil
}

func (db *cockroachDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *cockroachDB) CleanupThread(_ context.Context) {}

func (db *cockroachDB) Read(ctx context.Context, table, key string, fields []string) (map[string][]byte, error) {
	if len(fields) == 0 {
		fields = db.fieldKeys
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE YCSB_KEY = $1", strings.Join(fields, ","), db.table)
	row := db.db.QueryRow(ctx, query, key)

	values := make([]interface{}, len(fields))
	pointers := make([][]byte, len(fields))
	for i := range values {
		values[i] = &pointers[i]
	}

	if err := row.Scan(values...); err != nil {
		return nil, err
	}

	result := make(map[string][]byte)
	for i, f := range fields {
		result[f] = pointers[i]
	}
	return result, nil
}

func (db *cockroachDB) Insert(ctx context.Context, table, key string, values map[string][]byte) error {
	var fieldNames []string
	var placeholders []string
	args := []interface{}{key}
	fieldNames = append(fieldNames, "YCSB_KEY")
	placeholders = append(placeholders, "$1")

	i := 2
	for k, v := range values {
		fieldNames = append(fieldNames, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, v)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", db.table, strings.Join(fieldNames, ","), strings.Join(placeholders, ","))
	_, err := db.db.Exec(ctx, query, args...)
	return err
}

func (db *cockroachDB) Update(ctx context.Context, table, key string, values map[string][]byte) error {
	var setClauses []string
	args := []interface{}{}
	i := 1
	for k, v := range values {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", k, i))
		args = append(args, v)
		i++
	}
	args = append(args, key)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE YCSB_KEY = $%d", db.table, strings.Join(setClauses, ","), i)
	_, err := db.db.Exec(ctx, query, args...)
	return err
}

func (db *cockroachDB) Delete(ctx context.Context, table, key string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE YCSB_KEY = $1", db.table)
	_, err := db.db.Exec(ctx, query, key)
	return err
}

func (db *cockroachDB) Scan(ctx context.Context, table, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func init() {
	ycsb.RegisterDBCreator("cockroach", cockroachCreator{})
}
