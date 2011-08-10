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

import edu.ucsc.dbtune.connectivity.DatabaseConnection
import edu.ucsc.dbtune.metadata.extraction.MetaDataExtractor
import edu.ucsc.dbtune.metadata.extraction.GenericJDBCExtractor
import edu.ucsc.dbtune.metadata.extraction.PGExtractor

import edu.ucsc.dbtune.connectivity.JdbcConnectionManager._

import java.util.Properties

/**
 * This class provides a scala mirror of the core.metadata.Configuration class
 */
class Configuration(c:CoreConfiguration) extends CoreConfiguration(c) {
  var indexes:List[Index] = Index.asScalaIndex(c.getIndexes())

  override def toString = indexes.toString
}
