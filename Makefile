export CC=go build
export GOOS=linux
export GOARCH=amd64
export MODULE=tkNaloga04

SUBDIRS = proto client replicator controller
.PHONY: clean subdirs $(SUBDIRS)

subdirs:
	@for dir in $(SUBDIRS); do \
		cd $(PWD)/$$dir && $(MAKE); \
	done
	
all: subdirs

clean:
	@for dir in $(SUBDIRS); do \
		cd $(PWD)/$$dir && $(MAKE) clean; \
	done
