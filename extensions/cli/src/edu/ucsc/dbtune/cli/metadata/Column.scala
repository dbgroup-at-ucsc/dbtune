/*
 ******************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 ******************************************************************************/
package edu.ucsc.dbtune.cli.metadata

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

/**
 * This class provides a scala mirror of the core.metadata.Column class
 */
class Column(c:CoreColumn) extends CoreColumn(c) {
}

object Column {
  /**
   * Creates a list of column objects
   *
   * @param javaColumnList
   *    list of columns
   */
  def asScalaColumn( coreColumns:java.util.List[CoreColumn] ) : List[Column] =  {
    var cliColumns = List[Column]()

    for (x <- asScalaBuffer(coreColumns)) {
      cliColumns ::= new Column(x)
    }

    return cliColumns
  }
}
