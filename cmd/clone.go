package cmd

import "github.com/spf13/cobra"

var cloneCmd = &cobra.Command{
	Use:     "clone",
	Short:   "Creates an empty clone of the table you want to backup to",
	PreRunE: checkRequiredCloneFlags,
	RunE:    cloneTable,
}

func checkRequiredCloneFlags(cmd *cobra.Command, args []string) error {
	if sourceTable == "" {
		return flagError("sourceTable")
	}
	if targetTable == "" {
		return flagError("targetTable")
	}
	return nil
}

func cloneTable(cmd *cobra.Command, args []string) error {
	a := newAws()
	return a.CreateTableFrom(sourceTable, targetTable)
}
