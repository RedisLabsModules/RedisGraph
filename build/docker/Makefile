
ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

# OSNICK=buster|stretch|bionic|xenial|centos7
OSNICK ?= buster

REPO=redisgraph

INT_BRANCHES=2.2 2.0 1.2 1.0

ART_DIR=$(ROOT)/bin/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
