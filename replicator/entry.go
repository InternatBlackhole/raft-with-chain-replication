package replicator

import (
	"tkNaloga04/rpc"
)

type entry struct {
	key             string
	value           string
	commitedVersion uint32
	//if equal to version, then it is commited (and not dirty), else that version is the newest in system
	pendingVersion uint32
}

func (e entry) isDirty() bool {
	return e.pendingVersion > e.commitedVersion
}

func (e entry) isUpToDate() bool {
	return e.pendingVersion == e.commitedVersion
}

func newEntry(ent *rpc.InternalEntry) entry {
	return entry{key: ent.Key, value: ent.Value, commitedVersion: ent.Version, pendingVersion: ent.Version}
}
