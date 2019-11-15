package dupreq

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"reflect"
	"time"
)

const prefix = "ref_so_"

func GetNewRedisPool() *redis.Pool {
	host := os.Getenv("REDIS_HOST")
	password := os.Getenv("REDIS_PASSWORD")
	redisPool := &redis.Pool{
		MaxIdle:     2,
		IdleTimeout: 60 * time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", host+":6379", redis.DialPassword(password))
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) > time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return redisPool
}

type StorageService struct {
	redis *redis.Pool
}

func NewStorageService(redisPool *redis.Pool) *StorageService {
	if redisPool == nil {
		redisPool = GetNewRedisPool()
	}
	return &StorageService{
		redis: redisPool,
	}
}

type Request struct {
	Begin      time.Time
	End        time.Time
	StatusCode int
	Body       []byte
}

func (r Request) ToGOB64() string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(r)
	if err != nil {
		log.Println(`failed gob Encode`, err)
	}
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

func (r *Request) FromGOB64(str string) {
	by, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		log.Println(`failed base64 Decode`, err)
	}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err = d.Decode(&r)
	if err != nil {
		log.Println(`failed gob Decode`, err)
	}
}

func (s *StorageService) SaveIncomingRequestWithPrefix(prefix string, requestID string, request Request) error {
	return s.SaveIncomingRequest(prefix+"_"+requestID, request)
}

func (s *StorageService) SaveIncomingRequest(requestID string, request Request) error {
	db := s.redis.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	reply, err := db.Do("SETNX", prefix+requestID, request.ToGOB64())
	if err != nil {
		return err
	}

	fmt.Printf("[SaveIncomingRequest] reply: %+v, %s\n", reply, reflect.TypeOf(reply))
	r, ok := reply.(int64)
	if !ok || r == 0 {
		// already exists
		return errors.New("key already exists")
	}

	return nil
}

func (s *StorageService) GetRequestWithPrefix(prefix string, requestID string) (*Request, error) {
	return s.GetRequest(prefix + "_" + requestID)
}

func (s *StorageService) GetRequest(requestID string) (*Request, error) {
	db := s.redis.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	reply, err := db.Do("GET", prefix+requestID)
	if err != nil {
		return nil, err
	}

	r, err := redis.String(reply, nil)
	fmt.Printf("[GetRequest] reply: %s, %s\n", r, reflect.TypeOf(reply))
	if err != nil {
		return nil, errors.New("error parsing reply string: " + err.Error())
	}

	request := Request{}
	request.FromGOB64(r)

	return &request, nil
}

func (s *StorageService) SaveOutgoingRequestWithPrefix(prefix string, requestID string, request Request) error {
	return s.SaveOutgoingRequest(prefix+"_"+requestID, request)
}

func (s *StorageService) SaveOutgoingRequest(requestID string, request Request) error {
	db := s.redis.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	reply, err := db.Do("SETEX", prefix+requestID, int64(1*time.Hour/time.Second), request.ToGOB64())
	if err != nil {
		return err
	}

	fmt.Printf("[SaveOutgoingRequest] reply: %+v, %s\n", reply, reflect.TypeOf(reply))

	return nil
}
