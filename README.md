# Rookie-DB
Optimized a vanilla DBMS by implementing B+tree indexing, efficient join algoriths, improved performance by introducing concurrency and query optimization which resembles IBM System R, transaction and concurrency control with uses both coarse and fine granularity to allow high performance and high concurrency, and ARIES recovery to improve performance while maintaining the ACID properties and guarantee fault tolerance.

## Codebase Overview

The code is located in the
[`src/main/java/edu/berkeley/cs186/database`](src/main/java/edu/berkeley/cs186/database)
directory, while the tests are located in the
[`src/test/java/edu/berkeley/cs186/database`](src/test/java/edu/berkeley/cs186/database)
directory.
The following is a brief overview of each of the major sections of the codebase.

### [`cli`](src/main/java/edu/berkeley/cs186/database/cli)

The cli directory contains all the logic for the database's command line
interface. Running the main method of CommandLineInterface.java will create an
instance of the database and create a simple text interface that you can send
and review the results of queries in.

#### [`cli/parser`](src/main/java/edu/berkeley/cs186/database/cli/parser)

The subdirectory cli/parser contains a lot of scary looking code! Don't be
intimidated, this is all generated automatically from the file RookieParser.jjt
in the root directory of the repo. The code here handles the logic to convert
from user inputted queries (strings) into a tree of nodes representing the query
(parse tree).

#### [`cli/visitor`](src/main/java/edu/berkeley/cs186/database/cli/visitor)

The subdirectory cli/visitor contains classes that help traverse the trees
created from the parser and create objects that the database can work with
directly.

### [`common`](src/main/java/edu/berkeley/cs186/database/common)

The `common` directory contains bits of useful code and general interfaces that
are not limited to any one part of the codebase.

### [`concurrency`](src/main/java/edu/berkeley/cs186/database/concurrency)

The `concurrency` directory contains a implementation of multigranularity
locking to the database.

### [`databox`](src/main/java/edu/berkeley/cs186/database/databox)

The `databox` directory contains classes which represents values stored in
a database, as well as their types. The various `DataBox` classes represent
values of certain types, whereas the `Type` class represents types used in the
database.

### [`index`](src/main/java/edu/berkeley/cs186/database/index)

The `index` directory contains an implementation of B+ tree indices.

### [`memory`](src/main/java/edu/berkeley/cs186/database/memory)

The `memory` directory contains classes for managing the loading of data
into and out of memory (in other words, buffer management).

The `BufferFrame` class represents a single buffer frame (page in the buffer
pool) and supports pinning/unpinning and reading/writing to the buffer frame.
All reads and writes require the frame be pinned (which is often done via the
`requireValidFrame` method, which reloads data from disk if necessary, and then
returns a pinned frame for the page).

The `BufferManager` interface is the public interface for the buffer manager of
our DBMS.

The `BufferManagerImpl` class implements a buffer manager using
a write-back buffer cache with configurable eviction policy. It is responsible
for fetching pages (via the disk space manager) into buffer frames, and returns
Page objects to allow for manipulation of data in memory.

The `Page` class represents a single page. When data in the page is accessed or
modified, it delegates reads/writes to the underlying buffer frame containing
the page.

The `EvictionPolicy` interface defines a few methods that determine how the
buffer manager evicts pages from memory when necessary. Implementations of these
include the `LRUEvictionPolicy` (for LRU) and `ClockEvictionPolicy` (for clock).

### [`io`](src/main/java/edu/berkeley/cs186/database/io)

The `io` directory contains classes for managing data on-disk (in other words,
disk space management).

The `DiskSpaceManager` interface is the public interface for the disk space
manager of our DBMS.

The `DiskSpaceMangerImpl` class is the implementation of the disk space
manager, which maps groups of pages (partitions) to OS-level files, assigns
each page a virtual page number, and loads/writes these pages from/to disk.

### [`query`](src/main/java/edu/berkeley/cs186/database/query)

The `query` directory contains classes for managing and manipulating queries.
The various operator classes are query operators (pieces of a query).

The `QueryPlan` class represents a plan for executing a query (which we will be
covering in more detail later in the semester). It currently executes the query
as given (runs things in logical order, and performs joins in the order given),
but you will be implementing a query optimizer to run the query in a more
efficient manner.

### [`recovery`](src/main/java/edu/berkeley/cs186/database/recovery)

The `recovery` directory contains animplemention of database recovery
a la ARIES.

### [`table`](src/main/java/edu/berkeley/cs186/database/table)

The `table` directory contains classes representing entire tables and records.

The `Table` class is, as the name suggests, a table in our database. See the
comments at the top of this class for information on how table data is layed out
on pages.

The `Schema` class represents the _schema_ of a table (a list of column names
and their types).

The `Record` class represents a record of a table (a single row). Records are
made up of multiple DataBoxes (one for each column of the table it belongs to).

The `RecordId` class identifies a single record in a table.


The `PageDirectory` class is an implementation of a heap file that uses a page directory.

#### [`table/stats`](src/main/java/edu/berkeley/cs186/database/table/stats)

The `table/stats` directory contains classes for keeping track of statistics of
a table. These are used to compare the costs of different query plans.

### [`Transaction.java`](src/main/java/edu/berkeley/cs186/database/Transaction.java)

The `Transaction` interface is the _public_ interface of a transaction - it
contains methods that users of the database use to query and manipulate data.

This interface is partially implemented by the `AbstractTransaction` abstract
class, and fully implemented in the `Database.Transaction` inner class.

### [`TransactionContext.java`](src/main/java/edu/berkeley/cs186/database/TransactionContext.java)

The `TransactionContext` interface is the _internal_ interface of a transaction -
it contains methods tied to the current transaction that internal methods
(such as a table record fetch) may utilize.

The current running transaction's transaction context is set at the beginning
of a `Database.Transaction` call (and available through the static
`getCurrentTransaction` method) and unset at the end of the call.

This interface is partially implemented by the `AbstractTransactionContext` abstract
class, and fully implemented in the `Database.TransactionContext` inner class.

### [`Database.java`](src/main/java/edu/berkeley/cs186/database/Database.java)

The `Database` class represents the entire database. It is the public interface
of our database - users of our database can use it like a Java library.

All work is done in transactions, so to use the database, a user would start
a transaction with `Database#beginTransaction`, then call some of
`Transaction`'s numerous methods to perform selects, inserts, and updates.
