package git

import (
	"fmt"
	"strings"

	"github.com/operator-framework/rukpak/api/v1alpha1"
)

const (
	defaultDirectory = "./manifests"
	defaultBranch    = "main"
)

func CheckoutCommand(source v1alpha1.GitSource) string {
	var checkoutCommand string
	var repository = source.Repository
	var repositoryName = strings.Split(repository, "/")[4]
	var directory = source.Directory
	var branch = source.Ref.Branch
	var commit = source.Ref.Commit
	var tag = source.Ref.Tag

	if directory == "" {
		directory = defaultDirectory
	}

	if branch == "" {
		branch = defaultBranch
	}

	if commit != "" {
		checkoutCommand = fmt.Sprintf("git clone %s && cd %s && git checkout %s && cp -r %s/* /manifests",
			repository, repositoryName, commit, directory)
		return checkoutCommand
	}

	if tag != "" {
		checkoutCommand = fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout tags/%s && cp -r %s/* /manifests",
			tag, repository, repositoryName, tag, directory)
		return checkoutCommand
	}

	checkoutCommand = fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout %s && cp -r %s/* /manifests",
		branch, repository, repositoryName, branch, directory)
	return checkoutCommand
}
