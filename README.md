# DAOS Seismic Graph (DSG)
<p>
  <img src="https://img.shields.io/pypi/status/Django.svg" alt="stable"/>
</p>

A C based, scalable, seismic extention on top of the daos file system providing a high level API for efficient, high performant seismic data access and manipulation operations.

## Table of content
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Hierarchy](#project-hierarchy)
- [Installation](#installation)
- [Versioning](#versioning)


## Features

* conversions from segy file format to DAOS seismic graph (DSG)
* Ability to manipulate traces headers through DAOS seismic API:
	* Getting headers values.
	* Setting headers values.
	* Changing headers values.
	* Windowing/filtering headers based on specific headers min & max values.
	* Sorting headers based on one or more headers values.
	* Getting the range of all headers or of specific headers.
* Ability to manipulate traces data through DAOS seismic API
	* Getting traces data arrays.
	* Setting traces data arrays.

## Prerequisites

* **Scons**\
Scons version 3.1.2 or higher.

* **DAOS**\
DAOS based on the commit number 866760fcea49463a98433d8d7a90a9fbb0875eb5 was used during the testing of this work.
[DAOS installation guide] (https://daos-stack.github.io/admin/installation/)

* **Cmocka**\
Cmocka unit testing framework.

## Project Hierarchy

* **```include```**\
Folder containing all the headers of the system. Contains ReadMe explaining the internal file structure of the project.

* **```src```**\
Folder containing all the source files of the system. Follows same structure as the include.

* **```tests```**\
Folder containing all the tests of the system. Follows same structure as the include.

## Installation

* Ensure all required prerequisites are installed.
* Ensure DAOS is already built.
* Go to the project directory.
* Export needed paths 
```shell script
Source scripts/env.sh
```
* Build DSG API
```shell script
scons
```

## Versioning

When installing DAOS Seismic Graph, require it's version. For us, this is what ```major.minor.patch``` means:
- ```major``` - **MAJOR breaking changes**; includes major new features, major changes in how the whole system works, and complete rewrites; it allows us to _considerably_ improve the product, and add features that were previously impossible.
- ```minor``` - **MINOR breaking changes**; it allows us to add big new features, for free.
- ```patch``` - **NO breaking changes**; includes bug fixes and non-breaking new features.
