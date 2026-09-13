// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package downsampler_plugin_test

import (
	"context"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// A message built via SetStructured() keeps timestamp_ms as whatever native
// Go type the caller supplied (e.g. int64) - that's what every other test in
// this package sends. A message built from real JSON bytes instead decodes
// timestamp_ms as json.Number, which is exactly what happens in a real
// pipeline once a message has already crossed one processor/broker boundary
// before reaching the downsampler. This gap is why the bug wasn't caught
// elsewhere: existing tests only observe json.Number on the way *out* of the
// downsampler, never feed it in as timestamp_ms on the way in.
var _ = Describe("Downsampler timestamp_ms type handling", func() {
	It("accepts timestamp_ms decoded as json.Number from real JSON bytes", func() {
		msgHandler, messages, cleanup := SetupDownsamplerStream(`
downsampler:
  default:
    deadband:
      threshold: 0`)
		DeferCleanup(cleanup)

		nowMs := time.Now().UnixMilli()
		body := `{"value": 42, "timestamp_ms": ` + strconv.FormatInt(nowMs, 10) + `}`
		msg := service.NewMessage([]byte(body))
		msg.MetaSet("umh_topic", "test.json_number_timestamp")

		err := msgHandler(context.Background(), msg)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return len(*messages) }, "2s").Should(Equal(1))

		structured, err := (*messages)[0].AsStructured()
		Expect(err).NotTo(HaveOccurred())
		Expect(structured.(map[string]interface{})["value"]).To(BeEquivalentTo(42))
	})
})
