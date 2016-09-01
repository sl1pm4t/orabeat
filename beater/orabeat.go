package orabeat

import (
	"database/sql"
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"gopkg.in/rana/ora.v3"
	"strings"
	"sync"
	"time"
)

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

type Orabeat struct {
	period time.Duration

	ObConfig ConfigSettings
	events   publisher.Client

	statsfilter []string
	sysStats    bool
	sesStats    bool

	host     string
	port     uint16
	service  string
	user     string
	password string

	db          *sql.DB
	sysStatStmt *sql.Stmt
	sesStatStmt *sql.Stmt

	prevSystemStats   map[string]float64
	sessionStatistics *SessionStatistics

	done chan struct{}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func New() *Orabeat {
	return &Orabeat{}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// C O N F I G
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) Config(b *beat.Beat) error {

	err := cfgfile.Read(&ob.ObConfig, "")
	if err != nil {
		logp.Err("Error reading configuration file: %v", err)
		return err
	}

	if ob.ObConfig.Input.Period != nil {
		ob.period = time.Duration(*ob.ObConfig.Input.Period) * time.Second
	} else {
		ob.period = 10 * time.Second
	}

	ob.host = ob.ObConfig.Input.Host
	ob.port = ob.ObConfig.Input.Port
	ob.service = ob.ObConfig.Input.Service
	ob.user = ob.ObConfig.Input.User
	ob.password = ob.ObConfig.Input.Password

	if ob.ObConfig.Input.Stats.System != nil {
		ob.sysStats = *ob.ObConfig.Input.Stats.System
	} else {
		ob.sysStats = true
	}
	if ob.ObConfig.Input.Stats.Session != nil {
		ob.sesStats = *ob.ObConfig.Input.Stats.Session
	} else {
		ob.sesStats = true
	}

	logp.Debug("orabeat", "Init orabeat")
	logp.Debug("orabeat", "Follow stats %q\n", ob.statsfilter)
	logp.Debug("orabeat", "Period %v\n", ob.period)
	logp.Debug("orabeat", "System statistics %t\n", ob.sysStats)
	logp.Debug("orabeat", "Session statistics %t\n", ob.sesStats)

	return nil
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// S E T U P
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) Setup(b *beat.Beat) error {

	var err error

	ob.events = b.Events
	ob.done = make(chan struct{})

	// Setup DB connection
	drvCfg := ora.NewDrvCfg()
	drvCfg.Env.StmtCfg.SetPrefetchRowCount(5000)
	ora.SetDrvCfg(drvCfg)

	// Craft Connection String
	conString := fmt.Sprintf("%s/%s@%s:%d/%s", ob.user, ob.password, ob.host, ob.port, ob.service)
	conStringPWMasked := fmt.Sprintf("%s/<password>@%s:%d/%s", ob.user, ob.password, ob.host, ob.port, ob.service)
	logp.Debug("orabeat", "Conn String: %s", conStringPWMasked)

	// Open connection and ping DB to test
	ob.db, err = sql.Open("ora", conString)
	if err != nil {
		logp.Err("Failed to open DB connection: %v", err)
		return err
	}

	err = ob.db.Ping()
	if err != nil {
		logp.Err("DB Ping Failed.")
		return err
	}

	// Init prepared statement
	if ob.sesStats {
		ob.sesStatStmt, err = ob.db.Prepare(sesStatQuery)
		ob.sessionStatistics = NewSessionStatistics()
	}

	// Init prepared statement
	if ob.sysStats {
		ob.sysStatStmt, err = ob.db.Prepare(sysStatQuery)
		ob.prevSystemStats = make(map[string]float64)
	}

	return err
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// R U N
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) Run(b *beat.Beat) error {
	var err error

	ticker := time.NewTicker(ob.period)
	defer ticker.Stop()

	for {
		select {
		case <-ob.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		if ob.sysStats {
			//err = ob.exportSystemStats()
			go ob.exportSystemStats()
			//if err != nil {
			//	logp.Err("Error reading system stats: %v", err)
			//	break
			//}
		}
		if ob.sesStats {
			//err = ob.exportSessionStats()
			go ob.exportSessionStats()
			//if err != nil {
			//	logp.Err("Error reading session stats: %v", err)
			//	break
			//}
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > ob.period.Nanoseconds() {
			logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
		}
	}

	return err
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// C L E A N U P
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) Cleanup(b *beat.Beat) error {
	if ob.sesStatStmt != nil {
		ob.sesStatStmt.Close()
	}
	if ob.sysStatStmt != nil {
		ob.sysStatStmt.Close()
	}
	if ob.db != nil {
		ob.db.Close()
	}

	return nil
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// S T O P
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) Stop() {
	close(ob.done)
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// S Y S T E M  S T A T I S T I C S
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) exportSystemStats() error {

	rows, err := ob.sysStatStmt.Query()

	if err != nil {
		logp.Err("%v", err)
		return err
	}
	defer rows.Close()

	var counter uint
	var ts string

	allStats := make(map[string]float64)
	allStatsChange := make(map[string]float64)
	for rows.Next() {
		counter += 1

		var statName string
		//var class int
		var value float64
		err := rows.Scan(&ts, &statName, &value)
		if err != nil {
			logp.Err("%v", err)
			return err
		}
		logp.Debug("orabeat", "System Metric: %s\t|%s -> %v", ts, statName, value)

		var change float64
		cleanedStatName := cleanupStatName(statName)
		change = 0
		if prev, ok := ob.prevSystemStats[cleanedStatName]; ok {
			change = value - prev
		}
		allStats[cleanedStatName] = value
		allStatsChange[cleanedStatName] = change
	}

	event := common.MapStr{
		"@timestamp": common.MustParseTime(ts),
		"type":       "sysstat",
		"stats":      allStats,
		"change":     allStatsChange,
	}

	ob.events.PublishEvent(event)
	ob.prevSystemStats = allStats

	logp.Info("orabeat -> Retrieved %d system stats.", counter)

	return nil
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// S E S S I O N  S T A T I S T I C S
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

type SessionStatistics struct {
	Sessions map[string]*Session
	l        sync.RWMutex
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

type Session struct {
	sid         uint32
	serial      uint32
	machine     string
	username    string
	program     string
	prevMetrics map[string]float64
	metrics     map[string]float64
	change      map[string]float64
	l           sync.RWMutex
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func NewSessionStatistics() *SessionStatistics {
	return &SessionStatistics{
		Sessions: make(map[string]*Session),
		l:        sync.RWMutex{},
	}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ss *SessionStatistics) ensureSession(sid uint32, serial uint32, machine, username, program string) *Session {
	var sess *Session
	var ok bool
	sessId := sessionId(sid, serial)

	ss.l.RLock()
	sess, ok = ss.Sessions[sessId]
	ss.l.RUnlock()

	if !ok {
		ss.l.Lock()
		defer ss.l.Unlock()

		if _, ok2 := ss.Sessions[sessId]; !ok2 {
			sess = &Session{
				sid:      sid,
				serial:   serial,
				machine:  machine,
				username: username,
				program:  program,
				metrics:  make(map[string]float64),
				change:   make(map[string]float64),
				l:        sync.RWMutex{},
			}
			ss.Sessions[sessId] = sess
		}
	}

	return sess
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ss *SessionStatistics) rollMetrics() {
	for _, sess := range ss.Sessions {
		sess.rollMetrics()
	}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func sessionId(sid, serial uint32) string {
	return fmt.Sprintf("%d,%d", sid, serial)
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (s *Session) addSessionStat(statName string, value float64) {
	s.l.Lock()
	defer s.l.Unlock()

	s.metrics[statName] = value

	var change float64
	change = 0
	if prev, ok := s.prevMetrics[statName]; ok {
		change = value - prev
	}
	s.change[statName] = change
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (s *Session) rollMetrics() {
	s.l.Lock()
	defer s.l.Unlock()
	s.prevMetrics = s.metrics
	s.metrics = make(map[string]float64)
}

func (s *Session) Metrics() map[string]float64 {
	s.l.RLock()
	defer s.l.RUnlock()

	newMap := make(map[string]float64)
	for k,v := range s.metrics {
		newMap[k] = v
	}

	return newMap
}

func (s *Session) Changes() map[string]float64 {
	s.l.RLock()
	defer s.l.RUnlock()

	newMap := make(map[string]float64)
	for k,v := range s.change {
		newMap[k] = v
	}

	return newMap
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

func (ob *Orabeat) exportSessionStats() error {

	startTime := time.Now()

	rows, err := ob.sesStatStmt.Query()
	if err != nil {
		logp.Err("%v", err)
		return err
	}
	defer rows.Close()

	// Gather all the session stats
	for rows.Next() {
		var machine, username, program, statName string
		var sid, serial uint32
		var value float64

		err := rows.Scan(&machine, &username, &program, &sid, &serial, &statName, &value)
		if err != nil {
			logp.Err("%v", err)
			return err
		}
		logp.Debug("orabeat", "Session Metric: %s\t%s,%s (%d) \t|%s -> %v", machine, username, sid, statName, value)

		sess := ob.sessionStatistics.ensureSession(sid, serial, machine, username, program)
		sess.addSessionStat(cleanupStatName(statName), value)
	}

	// Publish documents for each session
	for _, s := range ob.sessionStatistics.Sessions {
		sessInfo := common.MapStr{
			"sid":      s.sid,
			"serial#":  s.serial,
			"machine":  s.machine,
			"username": s.username,
			"program":  s.program,
		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "sesstat",
			"count":      1,
			"session":    sessInfo,
			"stats":      s.Metrics(),
			"change":     s.Changes(),
		}
		ob.events.PublishEvent(event)
	}

	logp.Info("orabeat -> Retrieved stats for %d sessions in %dms", len(ob.sessionStatistics.Sessions), time.Now().Unix()-startTime.Unix())

	ob.sessionStatistics.rollMetrics()
	return nil
}

func cleanupStatName(original string) string {
	copy := original
	copy = strings.Replace(copy, ".", "", -1)
	return copy
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//2016-03-16T01:27:39.047932Z
var sesStatQuery = `
select
	machine,
	username,
	program,
	a.sid,
	a.serial#,
	b.name,
	c.value
from v$session a, v$statname b, v$sesstat c
where b.STATISTIC# =c.STATISTIC#
and c.sid=a.sid
order by a.sid, a.serial#`

//and b.name like ':1'`

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

var sysStatQuery = `
select
	to_char(systimestamp, 'YYYY-MM-DD"T"HH24:MI:SSxFF3"Z"') as ts,
	n.name,
	s.value
from v$statname n ,  v$sysstat s
where n.statistic# = s.statistic#
order by n.name`
