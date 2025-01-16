// Copyright 2024 PingCAP, Inc.
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

package label

import "github.com/onsi/ginkgo/v2"

var (
	// Components
	Cluster = ginkgo.Label("component:Cluster")
	PD      = ginkgo.Label("component:PD")
	TiDB    = ginkgo.Label("component:TiDB")
	TiKV    = ginkgo.Label("component:TiKV")
	TiFlash = ginkgo.Label("component:TiFlash")

	// Priority
	P0 = ginkgo.Label("P0")
	P1 = ginkgo.Label("P1")
	P2 = ginkgo.Label("P2")

	// Operations
	Update  = ginkgo.Label("op:Update")
	Scale   = ginkgo.Label("op:Scale")
	Suspend = ginkgo.Label("op:Suspend")
)