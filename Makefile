APPS = kernel stdlib sasl erts ssl tools runtime_tools crypto inets \
	public_key mnesia syntax_tools compiler
COMBO_PLT = $(HOME)/.dobby_combo_dialyzer_plt

.PHONY: compile deps test clean ct build_plt check_plt

compile:
	./rebar get-deps compile

deps:
	./rebar get-deps

eunit: compile
	./rebar -v skip_deps=true eunit

ct: compile
	./rebar -v ct $(CTARGS)

clean:
	./rebar clean

build_plt: compile
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

check_plt: compile
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	dialyzer --plt $(COMBO_PLT) ebin

dev: compile
	erl -pa ebin -pa deps/*/ebin \
	-eval "application:ensure_all_started(dobby)." \
	-name dobby@127.0.0.1 \
	-setcookie dobby

compile test clean: rebar

rebar:
	wget -c http://github.com/rebar/rebar/wiki/rebar
	chmod +x $@
