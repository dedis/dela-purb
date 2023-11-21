package controller

import (
	"go.dedis.ch/dela/cli"
	"go.dedis.ch/dela/cli/node"
	"go.dedis.ch/kyber/v3/util/key"
	"reflect"
	"testing"
)

func TestNewController(t *testing.T) {
	type args struct {
		isPurbOn bool
	}
	tests := []struct {
		name string
		args args
		want node.Initializer
	}{
		// Test cases.
		{"DB with purb off", args{false}, minimalController{false, nil}},
		{"DB with purb on", args{true}, minimalController{true, nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewController(tt.args.isPurbOn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewController() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_minimalController_OnStart(t *testing.T) {
	type fields struct {
		isPurbOn bool
		keys     []*key.Pair
	}
	type args struct {
		flags cli.Flags
		inj   node.Injector
	}
	inj := node.NewInjector()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// Test cases.
		{"DB with purb off", fields{false, nil}, args{node.FlagSet{}, inj}, false},
		{"DB with purb on", fields{true, nil}, args{node.FlagSet{}, inj}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := minimalController{
				isPurbOn: tt.fields.isPurbOn,
				keys:     tt.fields.keys,
			}
			if err := m.OnStart(tt.args.flags, tt.args.inj); (err != nil) != tt.wantErr {
				t.Errorf("OnStart() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_minimalController_OnStop(t *testing.T) {
	type fields struct {
		isPurbOn bool
		keys     []*key.Pair
	}
	type args struct {
		inj node.Injector
	}
	inj := node.NewInjector()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// Test cases.
		{"DB with purb off", fields{false, nil}, args{inj}, false},
		{"DB with purb on", fields{true, nil}, args{inj}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := minimalController{
				isPurbOn: tt.fields.isPurbOn,
				keys:     tt.fields.keys,
			}
			if err := m.OnStop(tt.args.inj); (err != nil) != tt.wantErr {
				t.Errorf("OnStop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_minimalController_SetCommands(t *testing.T) {
	type fields struct {
		isPurbOn bool
		keys     []*key.Pair
	}
	type args struct {
		builder node.Builder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// Test cases.
		{"DB with purb off", fields{false, nil}, args{nil}},
		{"DB with purb on", fields{true, nil}, args{nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := minimalController{
				isPurbOn: tt.fields.isPurbOn,
				keys:     tt.fields.keys,
			}
			m.SetCommands(tt.args.builder)
		})
	}
}
