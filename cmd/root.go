// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
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

package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "s3-dynamo-restore",
	Short: "Restore backup to dynamo from S3",
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmd.AddCommand(cloneCmd)
	RootCmd.AddCommand(restoreCmd)
	RootCmd.AddCommand(s3Cmd)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.s3-dynamo-restore.yaml)")
	RootCmd.PersistentFlags().StringVarP(&sourceTable, "sourceTable", "", "", "Dynamo table that backups were originally written for")
	RootCmd.PersistentFlags().StringVarP(&targetTable, "targetTable", "", "", "Dynamo table to write backups to")
	RootCmd.PersistentFlags().StringVarP(&bucketName, "bucket", "b", "", "Bucket name to read backups from")
	RootCmd.PersistentFlags().StringVarP(&bucketPrefix, "prefix", "p", "/", "Bucket prefix that backups are written to")
	RootCmd.PersistentFlags().StringVarP(&startTime, "startTime", "s", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
	RootCmd.PersistentFlags().StringVarP(&endTime, "endTime", "e", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

	viper.SetConfigName(".s3-dynamo-restore") // name of config file (without extension)
	viper.AddConfigPath(os.Getenv("HOME"))    // adding home directory as first search path
	viper.AutomaticEnv()                      // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func flagError(flag string) error {
	return errors.New("Error: Missing required flag " + flag)
}
