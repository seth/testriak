.PHONY: deps

all: deps
	./rebar compile

deps:
	./rebar get-deps

test:
	./rebar eunit skip_deps=true
