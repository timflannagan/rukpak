package git

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/operator-framework/rukpak/api/v1alpha1"
)

const (
	defaultDirectory = "./manifests"
	defaultBranch    = "main"
)

type CheckoutCmd struct {
	v1alpha1.GitSource
}

func NewCheckoutCmd(s v1alpha1.GitSource) CheckoutCmd {
	return CheckoutCmd{GitSource: s}
}

func (c CheckoutCmd) String() (string, error) {
	var checkoutCommand string
	var repository = c.Repository
	var directory = c.Directory
	var branch = c.Ref.Branch
	var commit = c.Ref.Commit
	var tag = c.Ref.Tag

	repositoryName, err := c.ValidateRepo()
	if err != nil {
		return "", err
	}

	if directory == "" {
		directory = defaultDirectory
	}

	if branch == "" {
		branch = defaultBranch
	}

	if commit != "" {
		checkoutCommand = fmt.Sprintf("git clone %s && cd %s && git checkout %s && cp -r %s/* /manifests",
			repository, repositoryName, commit, directory)
		return checkoutCommand, nil
	}

	if tag != "" {
		checkoutCommand = fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout tags/%s && cp -r %s/* /manifests",
			tag, repository, repositoryName, tag, directory)
		return checkoutCommand, nil
	}

	checkoutCommand = fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout %s && cp -r %s/* /manifests",
		branch, repository, repositoryName, branch, directory)
	return checkoutCommand, nil
}

func (c CheckoutCmd) ValidateRepo() (string, error) {
	u, err := url.Parse(c.Repository)
	if err != nil {
		return "", fmt.Errorf("provided git repository url malformed: %s", err)
	}

	paths := strings.Split(u.Path, "/")
	if len(paths) == 1 {
		return "", errors.New("provided git repository url malformed: no path found")
	}

	repoName := paths[len(paths)-1]
	if strings.HasSuffix(repoName, ".git") {
		repoName = strings.TrimSuffix(repoName, ".git")
	}
	return repoName, nil
}
