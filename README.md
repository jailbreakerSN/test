Algorithme d analyse de mouvement de population
=========

Fort de plus de trente millions de clients, le **Groupe SONATEL** stocke journalièrement une très grande masse de données mobiles (appels, SMS, sonde…).Celles-ci s’avèrent être une importante mine d’informations, à condition qu’elles soient mise à la disposition de chercheurs et data scientist, qui seront à mesure d’y appliquer des algorithmes afin d’en extraire une multitude d’information utiles à la prise de décisions pour des clients comme les structures administratives, les entreprises, les organisations non gouvernementales etc... 
Pour ce Usecase, nous avons procédé  au développement d’un **algorithme d’analyse de mouvement de populations** avec les outils et technologies **Big Data**. Il sera question, à partir des relevés détaillés de communications ou **CDR** (Call Detail Records), de modéliser un algorithme capable de déterminer les lieux d’habitations (**home location**) et de travail (**work location**) des clients. Ainsi, l’application de cet algorithme sur des périodes antérieures et postérieures à la mise en place d’infrastructures comme l’autoroute à péage ou le Train Express Régionale permettra d’avoir une idée sur l’impact de ces travaux sur le désengorgement de la ville de Dakar, avec à l’appui des statistiques réelles sur les changements de lieux d’habitation et de travail des populations.

Table des Matieres
=================

<!--ts-->
   * [Algorithme d analyse de mouvement de population](#Algorithme-d-analyse-de-mouvement-de-population)
   * [Table des Matieres](#Table-des-Matieres)
   * [Installation](#installation)
   * [Usage](#usage)
      * [STDIN](#stdin)
      * [Local files](#local-files)
      * [Remote files](#remote-files)
      * [Multiple files](#multiple-files)
      * [Combo](#combo)
      * [Auto insert and update TOC](#auto-insert-and-update-toc)
   * [Tests](#tests)
   * [Dependency](#dependency)
<!--te-->


Installation
============

```bash
$ wget https://raw.githubusercontent.com/ekalinin/github-markdown-toc/master/gh-md-toc
$ chmod a+x gh-md-toc
```

Usage
=====


STDIN
-----

Here's an example of TOC creating for markdown from STDIN:

```bash
➥ cat ~/projects/Dockerfile.vim/README.md | ./gh-md-toc -
  * [Dockerfile.vim](#dockerfilevim)
  * [Screenshot](#screenshot)
  * [Installation](#installation)
        * [OR using Pathogen:](#or-using-pathogen)
        * [OR using Vundle:](#or-using-vundle)
  * [License](#license)
```

Local files
-----------

Here's an example of TOC creating for a local README.md:

```bash
➥ ./gh-md-toc ~/projects/Dockerfile.vim/README.md                                                                                                                                                Вс. марта 22 22:51:46 MSK 2015

Table of Contents
=================

  * [Dockerfile.vim](#dockerfilevim)
  * [Screenshot](#screenshot)
  * [Installation](#installation)
        * [OR using Pathogen:](#or-using-pathogen)
        * [OR using Vundle:](#or-using-vundle)
  * [License](#license)
```

Remote files
------------

And here's an example, when you have a README.md like this:

  * [README.md without TOC](https://github.com/ekalinin/envirius/blob/f939d3b6882bfb6ecb28ef7b6e62862f934ba945/README.md)

And you want to generate TOC for it.

There is nothing easier:

```bash
➥ ./gh-md-toc https://github.com/ekalinin/envirius/blob/master/README.md

Table of Contents
=================

  * [envirius](#envirius)
    * [Idea](#idea)
    * [Features](#features)
  * [Installation](#installation)
  * [Uninstallation](#uninstallation)
  * [Available plugins](#available-plugins)
  * [Usage](#usage)
    * [Check available plugins](#check-available-plugins)
    * [Check available versions for each plugin](#check-available-versions-for-each-plugin)
    * [Create an environment](#create-an-environment)
    * [Activate/deactivate environment](#activatedeactivate-environment)
      * [Activating in a new shell](#activating-in-a-new-shell)
      * [Activating in the same shell](#activating-in-the-same-shell)
    * [Get list of environments](#get-list-of-environments)
    * [Get current activated environment](#get-current-activated-environment)
    * [Do something in environment without enabling it](#do-something-in-environment-without-enabling-it)
    * [Get help](#get-help)
    * [Get help for a command](#get-help-for-a-command)
  * [How to add a plugin?](#how-to-add-a-plugin)
    * [Mandatory elements](#mandatory-elements)
      * [plug_list_versions](#plug_list_versions)
      * [plug_url_for_download](#plug_url_for_download)
      * [plug_build](#plug_build)
    * [Optional elements](#optional-elements)
      * [Variables](#variables)
      * [Functions](#functions)
    * [Examples](#examples)
  * [Example of the usage](#example-of-the-usage)
  * [Dependencies](#dependencies)
  * [Supported OS](#supported-os)
  * [Tests](#tests)
  * [Version History](#version-history)
  * [License](#license)
  * [README in another language](#readme-in-another-language)
```

That's all! Now all you need — is copy/paste result from console into original
README.md.

And here is a result:

  * [README.md with TOC](https://github.com/ekalinin/envirius/blob/24ea3be0d3cc03f4235fa4879bb33dc122d0ae29/README.md)

Moreover, it's able to work with GitHub's wiki pages:

```bash
➥ ./gh-md-toc https://github.com/ekalinin/nodeenv/wiki/Who-Uses-Nodeenv

Table of Contents
=================

  * [Who Uses Nodeenv?](#who-uses-nodeenv)
    * [OpenStack](#openstack)
    * [pre-commit.com](#pre-commitcom)
```

Multiple files
--------------

It supports multiple files as well:

```bash
➥ ./gh-md-toc \
    https://github.com/aminb/rust-for-c/blob/master/hello_world/README.md \
    https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md \
    https://github.com/aminb/rust-for-c/blob/master/primitive_types_and_operators/README.md \
    https://github.com/aminb/rust-for-c/blob/master/unique_pointers/README.md

  * [Hello world](https://github.com/aminb/rust-for-c/blob/master/hello_world/README.md#hello-world)

  * [Control Flow](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#control-flow)
    * [If](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#if)
    * [Loops](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#loops)
    * [For loops](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#for-loops)
    * [Switch/Match](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#switchmatch)
    * [Method call](https://github.com/aminb/rust-for-c/blob/master/control_flow/README.md#method-call)

  * [Primitive Types and Operators](https://github.com/aminb/rust-for-c/blob/master/primitive_types_and_operators/README.md#primitive-types-and-operators)

  * [Unique Pointers](https://github.com/aminb/rust-for-c/blob/master/unique_pointers/README.md#unique-pointers)
```

Combo
-----

You can easily combine both ways:

```bash
➥ ./gh-md-toc \
    ~/projects/Dockerfile.vim/README.md \
    https://github.com/ekalinin/sitemap.s/blob/master/README.md

  * [Dockerfile.vim](~/projects/Dockerfile.vim/README.md#dockerfilevim)
  * [Screenshot](~/projects/Dockerfile.vim/README.md#screenshot)
  * [Installation](~/projects/Dockerfile.vim/README.md#installation)
        * [OR using Pathogen:](~/projects/Dockerfile.vim/README.md#or-using-pathogen)
        * [OR using Vundle:](~/projects/Dockerfile.vim/README.md#or-using-vundle)
  * [License](~/projects/Dockerfile.vim/README.md#license)

  * [sitemap.js](https://github.com/ekalinin/sitemap.js/blob/master/README.md#sitemapjs)
    * [Installation](https://github.com/ekalinin/sitemap.js/blob/master/README.md#installation)
    * [Usage](https://github.com/ekalinin/sitemap.js/blob/master/README.md#usage)
    * [License](https://github.com/ekalinin/sitemap.js/blob/master/README.md#license)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)
```

Auto insert and update TOC
--------------------------

Just put into a file these two lines:

```
<!--ts-->
<!--te-->
```

And run:

```bash
$ ./gh-md-toc --insert README.test.md

Table of Contents
=================

   * [gh-md-toc](#gh-md-toc)
   * [Installation](#installation)
   * [Usage](#usage)
      * [STDIN](#stdin)
      * [Local files](#local-files)
      * [Remote files](#remote-files)
      * [Multiple files](#multiple-files)
      * [Combo](#combo)
   * [Tests](#tests)
   * [Dependency](#dependency)

!! TOC was added into: 'README.test.md'
!! Origin version of the file: 'README.test.md.orig.2018-02-04_192655'
!! TOC added into a separate file: 'README.test.md.toc.2018-02-04_192655'


Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)
```

Now check the same file:

```bash
➜ grep -A15 "<\!\-\-ts" README.test.md
<!--ts-->
   * [gh-md-toc](#gh-md-toc)
   * [Table of contents](#table-of-contents)
   * [Installation](#installation)
   * [Usage](#usage)
      * [STDIN](#stdin)
      * [Local files](#local-files)
      * [Remote files](#remote-files)
      * [Multiple files](#multiple-files)
      * [Combo](#combo)
      * [Auto insert and update TOC](#auto-insert-and-update-toc)
   * [Tests](#tests)
   * [Dependency](#dependency)

<!-- Added by: <your-user>, at: 2018-02-04T19:38+03:00 -->

<!--te-->
```

Next time when your file will be changed just repeat the command (`./gh-md-toc
--insert ...`) and TOC will be refreshed again.


Tests
=====

Done with [bats](https://github.com/sstephenson/bats).
Useful articles:

  * https://blog.engineyard.com/2014/bats-test-command-line-tools
  * http://blog.spike.cx/post/60548255435/testing-bash-scripts-with-bats


How to run tests:

```bash
➥ make test                                                                                                                                                                                                      Пн. марта 23 13:59:27 MSK 2015
 ✓ TOC for local README.md
 ✓ TOC for remote README.md
 ✓ TOC for mixed README.md (remote/local)
 ✓ TOC for markdown from stdin
 ✓ --help
 ✓ --version

6 tests, 0 failures
```

Dependency
==========

  * curl or wget
  * awk (mawk, gawk is not supported)
  * grep
  * sed
  * bats (for unit tests)

Tested on Ubuntu 14.04/14.10 in bash/zsh.
