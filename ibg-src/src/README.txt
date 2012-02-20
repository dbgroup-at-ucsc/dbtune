INDEX INTERACTIONS
README.txt

At the top level, we have files like CandidateGenerationMain.java, 
AnalysisMain.java, etc, which are the different entry points of the
implementation. These different entry points allow us to save the
results of intermediate steps in files and reuse those results 
in multiple processes. For example, we can have a file that stores 
a list of index candidates, or a file that stores the index benefit 
graph. It is important to note that this organization only affects the
top-level package; the rest of the implementation can be used without
storing intermediate files.

Here is a  *partial* listing of the contents of this package, and their basic 
purpose. Although the list is incomplete, most of the important points are 
covered here to help you get started.

-- interaction/Configuration.java
This file has the global configuration of parameters. A big part of the
global configuration involves the paths of external files, which are used
to store the intermediate steps of the algorithm.

-- interaction/cand
This package is intended to generate candidate indexes for a workload. It
has some components that are specific to DB2.
If you are doing your own candidate generation, you can mostly ignore
this package.

-- interaction/db
The files in this package handle the connection to the database as well as
the basic steps for what-if optimization. If porting to a system other
than DB2, it may be possible to just revise this package and leave most 
of the other code unchanged. On the other hand, the API is somewhat system-
dependant. In particular, what-if optimization is done by first calling
fixCandidates(), then calling whatifOptimize() multiple times. This API 
is optimized for the way DB2 works.

-- interaction.ibg.log
This package is mainly used to write a file that tracks the incremental 
progress of the interaction analysis. It is mostly tied to the 
experimental results that we reported in the VLDB 2009 paper.

-- interaction.ibg.serial and interaction.ibg.parallel
These two package basically implement the same functionality, except that 
there are some differences in the APIs. The "serial" package is used if you
want to build the IBG and discover interactions in separate steps. The 
"parallel" package allows the IBG to be analyzed as it's being built, which
means that many interactions will be found in a short period of time.
