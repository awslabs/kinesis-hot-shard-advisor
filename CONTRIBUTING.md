# Contributing to Amazon Kinesis Hot Shard Advisor

Thanks for your interest in contributing to the Amazon Kinesis Hot Shard Advisor (KHS)! 💖

This document describes how to set up a development environment and submit your contributions.
Please read it over and let us know if it's not up-to-date (or, even better, submit a PR with your corrections 😉).

- [Development setup](#development-setup)
  - [Environment](#environment)
  - [Set upstream](#set-upstream)
  - [Building and testing](#building-and-testing)
  - [Generating mocks](#generating-mocks)
  - [Adding new dependencies](#adding-new-dependencies)
- [Where should I start?](#where-should-i-start)
- [Contributing code](#contributing-code)
- [Amazon Open Source Code of Conduct](#amazon-open-source-code-of-conduct)
- [Licensing](#licensing)

## Development setup

### Environment

- Make sure you are using Go 1.18 (`go version`).
- Fork the repository.
- Clone your forked repository locally.
- We use Go Modules to manage dependencies, so you can develop outside of your $GOPATH.

#### Set upstream

From the repository root run:

`git remote add upstream git@github.com:awslabs/kinesis-hot-shard-advisor`

`git fetch upstream`

### Building and testing

**Unit tests** makes up the majority of the testing and new code should include unit tests.
Ideally, these unit tests will be in the same package as the file they're testing and have full coverage (or as much is practical within a unit test).
Unit tests shouldn't make any network calls.

* Run `make` (This creates a standalone executable in the `build/khs` directory).
* Run `make test` to run the unit tests.

## Adding new dependencies

In general, we discourage adding new dependencies to the KHS. If there's a module you think the CLI could benefit from, first open a PR with your proposal.
We'll evaluate the dependency and the use case and decide on the next steps. To evaluate if we need to add the dependency please take a look at [https://research.swtch.com/deps](https://research.swtch.com/deps).

## Where should I start?

We're so excited you want to contribute to KHS! We welcome all PRs and will try to get to them as soon as possible.
The best place to start, though, is with filing an issue first. Filing an issue gives us some time to chat about the code you're keen on writing, and make sure that it's not already being worked on, or has already been discussed.

You can also check out our [issues queue](https://github.com/awslabs/kinesis-hot-shard-advisor/issues) to see all the known issues - this is a really great place to start.

If you want to get your feet wet, check out issues tagged with [good first issue](https://github.com/aws/kinesis-hot-shard-advisor/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).
These issues are great for folks who are looking to get started, but not sure where to start 😁.

## Contributing code
* Please check the existing issues to see if your feedback has already been reported.

* Let us know if you are interested in working on an issue by leaving a comment
on the issue in GitHub. This helps avoid multiple people unknowingly working on
the same issue.

* If you would like to propose a new feature, please open an issue on GitHub with
a detailed description. This enables us to collaborate on the feature design
more easily and increases the chances that your feature request will be accepted.

* New features should include full test coverage.

* New files should include the standard license  header.

* All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult GitHub Help for more
information on using pull requests.

## Amazon Open Source Code of Conduct

This code of conduct provides guidance on participation in Amazon-managed open source communities, and outlines the process for reporting unacceptable behavior. As an organization and community, we are committed to providing an inclusive environment for everyone. Anyone violating this code of conduct may be removed and banned from the community.

**Our open source communities endeavor to:**
* Use welcoming and inclusive language;
* Be respectful of differing viewpoints at all times;
* Accept constructive criticism and work together toward decisions;
* Focus on what is best for the community and users.

**Our Responsibility.** As contributors, members, or bystanders we each individually have the responsibility to behave professionally and respectfully at all times. Disrespectful and unacceptable behaviors include, but are not limited to:
The use of violent threats, abusive, discriminatory, or derogatory language;
* Offensive comments related to gender, gender identity and expression, sexual orientation, disability, mental illness, race, political or religious affiliation;
* Posting of sexually explicit or violent content;
* The use of sexualized language and unwelcome sexual attention or advances;
* Public or private [harassment](http://todogroup.org/opencodeofconduct/#definitions) of any kind;
* Publishing private information, such as physical or electronic address, without permission;
* Other conduct which could reasonably be considered inappropriate in a professional setting;
* Advocating for or encouraging any of the above behaviors.

**Enforcement and Reporting Code of Conduct Issues.**
Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting opensource-codeofconduct@amazon.com. All complaints will be reviewed and investigated and will result in a response that is deemed necessary and appropriate to the circumstances.

**Attribution.** _This code of conduct is based on the [template](http://todogroup.org/opencodeofconduct) established by the [TODO Group](http://todogroup.org/) and the Scope section from the [Contributor Covenant version 1.4](http://contributor-covenant.org/version/1/4/)._

## Licensing
Amazon Kinesis Hot Shard Advisor is released under an [MIT-0](https://github.com/aws/mit-0) license. Any code you submit will be released under that license.

For significant changes, we may ask you to sign a [Contributor License Agreement](http://en.wikipedia.org/wiki/Contributor_License_Agreement).
