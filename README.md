2ndQuadrant PostgreSQL development trees
===========

This repository contains development trees for various patches submitted to
PostgreSQL by 2ndQuadrant, in various stages of completion.

Do not use this repository for building normal PostgreSQL installs. Use the
official community PostgreSQL repository at

https://git.postgresql.org/gitweb/?p=postgresql.git;a=summary

or its github mirror at:

https://github.com/postgres/postgres

Branches and tags in this repository are linked to from:

* pgsql-hackers mailing list posts
* PostgreSQL commitfest entries
* blogs
* .. etc

and are used for collaborative development on features for submission
to core postgres.

Most people using this tree will want to clone upstream postgres, then
add this as a second remote, e.g.

    git remote add 2ndq https://github.com/2ndQuadrant/postgres.git
    git fetch 2ndq
