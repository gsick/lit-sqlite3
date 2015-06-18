LUAJIT_OS=$(shell luajit -e "print(require('ffi').os)")
LUAJIT_ARCH=$(shell luajit -e "print(require('ffi').arch)")
TARGET_DIR=$(LUAJIT_OS)-$(LUAJIT_ARCH)/

NAME=sqlite3

add:
	lit add ${PWD}

publish:
	lit publish ${PWD}

test-deps:
	cd tests && lit install
	[ -d tests/deps ] || mkdir -p tests/deps
	rm -f tests/deps/${NAME}
	ln -s ${PWD} tests/deps/${NAME}

test: test-deps
	cd tests && luvit main.lua
