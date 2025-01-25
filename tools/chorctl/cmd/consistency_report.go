/*
 * Copyright © 2025 Clyso GmbH
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

package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var consistencyReportCmd = &cobra.Command{
	Use:   "consistency report",
	Short: "list consistency checks",
	Long: `Example:
chorctl consistency report `,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewChorusClient(conn)
		res, err := client.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: args[0]})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get consistency check report")
		}

		storages := make([]string, 0, len(res.Check.Locations))
		for _, location := range res.Check.Locations {
			storages = append(storages, location.Storage)
		}

		hasErrors := len(res.Entries) > 0
		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.ConsistencyCheckReportBrief(res.Check, hasErrors))
		w.Flush()
		fmt.Fprintln(w)
		fmt.Fprintln(w, api.ConsistencyCheckReportHeader(storages))
		for _, entry := range res.Entries {
			fmt.Fprintln(w, api.ConsistencyCheckReportRow(storages, entry))
		}
		w.Flush()
	},
}

func init() {
	consistencyCmd.AddCommand(consistencyReportCmd)
}
