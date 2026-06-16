// Command contractgate is a harness-only probe that reports whether the Kafka
// contract compiled into this build (octo-lib searchmsg.SchemaVersion) carries
// the reader safety fields (spaceId/visibles/messageSeq) — i.e. whether the live
// consumer's safety gate (internal/consumer.Service.Run) will permit live
// ingestion or refuse to start.
//
// It exists so the local Kafka e2e harness (run.sh) can decide, BEFORE bringing
// up a stack and seeding, whether the live consumer path can run at all under the
// current contract. It imports the SAME predicate the production consumer uses
// (esindex.LiveContractCarriesSafetyFields), so it can never report a state that
// disagrees with production — and it reads nothing, writes nothing, and is not
// part of any production binary or deploy path.
//
//	exit 0  → "unlocked": contract carries safety fields; live ingestion permitted.
//	exit 10 → "gated":    contract lacks safety fields; live consumer refuses to
//	                       start (fail-closed). Use the backfill harness for v1.9 e2e.
package main

import (
	"fmt"
	"os"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

func main() {
	if esindex.LiveContractCarriesSafetyFields() {
		fmt.Printf("unlocked schema_version=%d safety_min=%d\n",
			searchmsg.SchemaVersion, esindex.SafetyFieldsSchemaVersion)
		os.Exit(0)
	}
	fmt.Printf("gated schema_version=%d safety_min=%d\n",
		searchmsg.SchemaVersion, esindex.SafetyFieldsSchemaVersion)
	os.Exit(10)
}
