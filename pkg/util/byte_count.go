/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
)

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func ParseBytes(in string) (int64, error) {
	in = strings.TrimSpace(in)
	bytes, err := strconv.Atoi(in)
	if err == nil {
		if bytes < 0 {
			return 0, fmt.Errorf("%w: invalid format - must be positive", dom.ErrInvalidArg)
		}
		return int64(bytes), nil
	}
	in = strings.TrimSuffix(in, "B")
	isBinary := strings.HasSuffix(in, "i")
	in = strings.TrimSuffix(in, "i")
	if len(in) == 0 {
		return 0, fmt.Errorf("%w: invalid format", dom.ErrInvalidArg)
	}
	exp := in[len(in)-1:]
	expVal := strings.Index("KMGTPE", exp) + 1
	if expVal == -1 {
		return 0, fmt.Errorf("%w: invalid format - unknown exponent %q", dom.ErrInvalidArg, exp)
	}
	in = in[:len(in)-1]
	val, err := strconv.ParseFloat(in, 64)
	if err != nil {
		return 0, err
	}
	if val < 0 {
		return 0, fmt.Errorf("%w: invalid format - must be positive", dom.ErrInvalidArg)
	}
	var res int64
	if isBinary {
		expVal *= 10
		mul := uint64(1 << uint(expVal))
		res = int64(val * float64(mul))
	} else {
		expVal *= 3
		mul := math.Pow10(expVal)
		res = int64(val * mul)
	}
	return res, nil
}
