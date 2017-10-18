package vitess

import (
        "database/sql"
        "net/http"
        "sync"
        "time"

        "github.com/m-masataka/beego-orm-vitess-plugin/vitessorm/session"
	"github.com/youtube/vitess/go/vt/vitessdriver"
)

type SessionStore struct {
	c      *sql.DB
	sid    string
	lock   sync.RWMutex
	values map[interface{}]interface{}
}

type Provider struct {
        maxlifetime int64
        savePath    string
}

var (
        // TableName store the session in MySQL
        TableName = "session"
        vitesspder = &Provider{}
)
// Set value in mysql session.
// it is temp value in map.
func (st *SessionStore) Set(key, value interface{}) error {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.values[key] = value
	return nil
}

// Get value from mysql session
func (st *SessionStore) Get(key interface{}) interface{} {
	st.lock.RLock()
	defer st.lock.RUnlock()
	if v, ok := st.values[key]; ok {
		return v
	}
	return nil
}

// Delete value in mysql session
func (st *SessionStore) Delete(key interface{}) error {
	st.lock.Lock()
	defer st.lock.Unlock()
	delete(st.values, key)
	return nil
}

// Flush clear all values in mysql session
func (st *SessionStore) Flush() error {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.values = make(map[interface{}]interface{})
	return nil
}

func (mp *Provider) SessionDestroy(sid string) error {
        c := mp.connectInit()
        c.Exec("DELETE FROM session where session_key=$1", sid)
        c.Close()
        return nil
}

// SessionID get session id of this mysql session store
func (st *SessionStore) SessionID() string {
	return st.sid
}

// SessionExist check postgresql session exist
func (mp *Provider) SessionExist(sid string) bool {
        c := mp.connectInit()
        defer c.Close()
        row := c.QueryRow("select session_data from session where session_key=$1", sid)
        var sessiondata []byte
        err := row.Scan(&sessiondata)
        return !(err == sql.ErrNoRows)
}

// SessionRelease save mysql session values to database.
// must call this method to save values to database.
func (st *SessionStore) SessionRelease(w http.ResponseWriter) {
	defer st.c.Close()
	b, err := session.EncodeGob(st.values)
	if err != nil {
		return
	}
	st.c.Exec("UPDATE "+TableName+" set `session_data`=?, `session_expiry`=? where session_key=?",
		b, time.Now().Unix(), st.sid)
}

func (mp *Provider) connectInit() *sql.DB {
	timeout := 10 * time.Second
        db, e := vitessdriver.Open("localhost:15991", "@master", timeout)
        if e != nil {
                return nil
        }
        return db
}

// SessionInit init postgresql session.
// savepath is the connection string of postgresql.
func (mp *Provider) SessionInit(maxlifetime int64, savePath string) error {
        mp.maxlifetime = maxlifetime
        mp.savePath = savePath
        return nil
}

// SessionRead get postgresql session by sid
func (mp *Provider) SessionRead(sid string) (session.Store, error) {
        c := mp.connectInit()
        row := c.QueryRow("select session_data from session where session_key=$1", sid)
        var sessiondata []byte
        err := row.Scan(&sessiondata)
        if err == sql.ErrNoRows {
                _, err = c.Exec("insert into session(session_key,session_data,session_expiry) values($1,$2,$3)",
                        sid, "", time.Now().Format(time.RFC3339))

                if err != nil {
                        return nil, err
                }
        } else if err != nil {
                return nil, err
        }

        var kv map[interface{}]interface{}
        if len(sessiondata) == 0 {
                kv = make(map[interface{}]interface{})
        } else {
                kv, err = session.DecodeGob(sessiondata)
                if err != nil {
                        return nil, err
                }
        }
        rs := &SessionStore{c: c, sid: sid, values: kv}
        return rs, nil
}

// SessionGC delete expired values in postgresql session
func (mp *Provider) SessionGC() {
        c := mp.connectInit()
        c.Exec("DELETE from session where EXTRACT(EPOCH FROM (current_timestamp - session_expiry)) > $1", mp.maxlifetime)
        c.Close()
}

// SessionRegenerate generate new sid for postgresql session
func (mp *Provider) SessionRegenerate(oldsid, sid string) (session.Store, error) {
        c := mp.connectInit()
        row := c.QueryRow("select session_data from session where session_key=$1", oldsid)
        var sessiondata []byte
        err := row.Scan(&sessiondata)
        if err == sql.ErrNoRows {
                c.Exec("insert into session(session_key,session_data,session_expiry) values($1,$2,$3)",
                        oldsid, "", time.Now().Format(time.RFC3339))
        }
        c.Exec("update session set session_key=$1 where session_key=$2", sid, oldsid)
        var kv map[interface{}]interface{}
        if len(sessiondata) == 0 {
                kv = make(map[interface{}]interface{})
        } else {
                kv, err = session.DecodeGob(sessiondata)
                if err != nil {
                        return nil, err
                }
        }
        rs := &SessionStore{c: c, sid: sid, values: kv}
        return rs, nil
}

func (mp *Provider) SessionAll() int {
        c := mp.connectInit()
        defer c.Close()
        var total int
        _, err := c.Query("SELECT count(*) as num from " + TableName)
        if err != nil {
                return 0
        }
        return total
}

func init() {
        session.Register("vitess", vitesspder)
}
