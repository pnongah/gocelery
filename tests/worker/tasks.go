package worker

import "fmt"

const GoFunc_Add = "add"
const GoFunc_Error = "throw_error"
const GoFuncKwargs_Add = "add-kwargs"
const GoFuncKwargs_Error = "throw_error-kwargs"
const PyFunc_Sub = "tasks.subtract"
const PyFunc_Error = "tasks.throw_error"
const PyFunc_Progress = "tasks.progress"

// ====================== Standalone Functions ================================

func Add(x int, y int) int {
	fmt.Println("adding...")
	return x + y
}

func ThrowError() error {
	return fmt.Errorf("go error")
}

// ====================== Kwarg Functions ================================

type adder struct {
}

type adderArgs struct {
	x int
	y int
}

func (a *adder) ParseKwargs(kwargs map[string]interface{}) (interface{}, error) {
	x := kwargs["x"].(float64)
	y := kwargs["y"].(float64)
	return &adderArgs{int(x), int(y)}, nil
}

func (a *adder) RunTask(input interface{}) (interface{}, error) {
	cast := input.(*adderArgs)
	return cast.x + cast.y, nil
}

type errorThrower struct{}

func (e *errorThrower) ParseKwargs(kwargs map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func (e *errorThrower) RunTask(input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("go kwargs error")
}
