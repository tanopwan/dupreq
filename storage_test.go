package dupreq_test

import (
	. "github.com/tanopwan/dupreq"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestRequestGOB64(t *testing.T) {
	request := Request{
		Begin:      time.Time{},
		End:        time.Time{},
		StatusCode: http.StatusOK,
		Body:       []byte("hello world"),
	}

	gob64 := request.ToGOB64()
	t.Logf("GOB64: %s", gob64)

	newRequest := Request{}
	newRequest.FromGOB64(gob64)

	if string(request.Body) != string(newRequest.Body) {
		t.Errorf("invalid parsing")
	}
	t.Log(string(newRequest.Body))
}

func TestSaveAndGetRequest(t *testing.T) {
	pool := GetNewRedisPool()
	db := pool.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	_, err := db.Do("DEL", "ref_so_1234")
	if err != nil {
		t.Errorf("test fail to prepare test data: %s\n", err.Error())
	}

	request := Request{
		Begin:      time.Now(),
		End:        time.Now(),
		StatusCode: http.StatusOK,
		Body:       []byte("hello world"),
	}

	service := NewStorageService(pool)
	err = service.SaveIncomingRequest("1234", request)
	if err != nil {
		t.Errorf("test fail to save incomming request: %s\n", err.Error())
	}

	getRequest, err := service.GetRequest("1234")
	if err != nil {
		t.Errorf("test fail to save get request: %s\n", err.Error())
	}

	t.Logf("getRequest: %+v\n", getRequest)
	if request.Begin.String() == getRequest.Begin.String() || request.End.String() == getRequest.End.String() {
		t.Errorf("test fail Begin/End")
	}

	if request.StatusCode != getRequest.StatusCode {
		t.Errorf("test fail StatusCode")
	}

	if string(request.Body) != string(getRequest.Body) {
		t.Errorf("test fail Body")
	}
}

func TestSaveRequestDup(t *testing.T) {
	pool := GetNewRedisPool()
	db := pool.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	_, err := db.Do("SET", "ref_so_1234", "ex")
	if err != nil {
		t.Errorf("test fail to prepare test data: %s\n", err.Error())
	}

	request := Request{
		Begin:      time.Time{},
		End:        time.Time{},
		StatusCode: http.StatusOK,
		Body:       []byte("hello world"),
	}

	service := NewStorageService(pool)
	err = service.SaveIncomingRequest("1234", request)
	if err == nil {
		t.Errorf("test fail to save incomming request: should be error\n")
	}
	t.Logf("expected error: nil\n")
}

func TestSaveAndUpdateRequest(t *testing.T) {
	pool := GetNewRedisPool()
	db := pool.Get()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close redis connection after create session with reason: %s\n", err.Error())
		}
	}()

	_, err := db.Do("DEL", "ref_so_1234")
	if err != nil {
		t.Errorf("test fail to prepare test data: %s\n", err.Error())
	}

	request := Request{
		Begin:      time.Now(),
		End:        time.Time{},
		StatusCode: 0,
		Body:       nil,
	}

	service := NewStorageService(pool)
	err = service.SaveIncomingRequest("1234", request)
	if err != nil {
		t.Errorf("test fail to save incomming request: %s\n", err.Error())
	}

	time.Sleep(time.Second)

	request.End = time.Now()
	request.StatusCode = http.StatusOK
	request.Body = []byte("hello world")

	err = service.SaveOutgoingRequest("1234", request)
	if err != nil {
		t.Errorf("test fail to save request: %s\n", err.Error())
	}

	getRequest, err := service.GetRequest("1234")
	if err != nil {
		t.Errorf("test fail to get request: %s\n", err.Error())
	}

	t.Logf("getRequest: %+v\n", getRequest)
	if request.Begin.String() == getRequest.Begin.String() || request.End.String() == getRequest.End.String() {
		t.Errorf("test fail Begin/End")
	}

	if 200 != getRequest.StatusCode {
		t.Errorf("test fail StatusCode")
	}

	if "hello world" != string(getRequest.Body) {
		t.Errorf("test fail Body")
	}
}
