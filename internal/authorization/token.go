package authorization

import (
	"encoding/json"
	"fmt"

	"github.com/muratmirgun/yordam/internal/protocol"
)

// CommittedToken is intentionally opaque. Only Service.Issue can construct a
// valid value after independently reading the named committed transactions.
type CommittedToken struct {
	valid               bool
	kind                string
	activityID          protocol.ActivityID
	controlOperationID  protocol.ControlOperationID
	callID              string
	planDigest          protocol.Digest
	requestDigest       protocol.Digest
	dispatchDigest      protocol.Digest
	runtimeGenerationID protocol.RuntimeGenerationID
	nonce               protocol.DecisionNonce
	revocationEpoch     uint64
}

func (t CommittedToken) ValidFor(activity protocol.ActivityID, callID string, plan, request, dispatch protocol.Digest, generation protocol.RuntimeGenerationID, nonce protocol.DecisionNonce) bool {
	return t.valid && t.activityID == activity && t.callID == callID && t.planDigest == plan && t.requestDigest == request && t.dispatchDigest == dispatch && t.runtimeGenerationID == generation && t.nonce == nonce
}

// MarshalJSON prevents tokens from crossing a serialization boundary.
func (CommittedToken) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("committed authorization tokens cannot be serialized")
}

var _ json.Marshaler = CommittedToken{}
