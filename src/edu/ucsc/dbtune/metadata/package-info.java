/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
/**
 * <!-- html generated with pandoc -->
 * 
 * <p>This package handles the terminology with regards to database metadata. The <a href="http://en.wikipedia.org/wiki/Sql">SQL Standard</a> defines a way in which a database object should be qualified but, as usual, each DBMS implements things differently. This gives rise to a term being used differently by each DBMS to refer to the same logical concept.</p>
 * <p>The DBTune project relies heavily in the use of Java and the JDBC framework, so the terminology used in the project is the same one used in the JDBC framework, which in turns is based in the SQL standard.</p>
 * <h1 id="definition-of-database-metadata">Definition of Database Metadata</h1>
 * <p>The SQL standard defines the following database object containment hierarchy:</p>
 * <ul>
 * <li>Catalog
 * <ul>
 * <li>Schema
 * <ul>
 * <li>Table
 * <ul>
 * <li>Column</li>
 * </ul></li>
 * <li>Index
 * <ul>
 * <li>Column</li>
 * </ul></li>
 * <li>Constraints</li>
 * <li>Privileges</li>
 * <li>Views</li>
 * <li>...</li>
 * </ul></li>
 * </ul></li>
 * </ul>
 * <p>Some DBMSs use the <em>Catalog</em> term to refer to a database (a specific database contained in a DBMS), while some others use it to refer to the system's schemata (a.k.a. system tables where metadata is stored), i.e. the <code>information_schema</code> as defined by th SQL standard. In DBTune, we refer to it as the metadata repository, or just the database's metadata and use the <em>Catalog</em> term to refer to the highest container in the hierarchy, that is, we use <em>Catalog</em> as a synonym for what is commonly referred to as a database. The metadata is composed by the information about the database objects contained in a database and DBTune uses the hierarchy above to represent it internally (as Java objects). The <code>metadata</code> package implements this terminology.</p>
 * <p>Following the SQL standard, the following definitions are used in DBTune:</p>
 * <dl>
 * <dt><em>Catalog</em></dt>
 * <dd>A container of schemas.
 * </dd>
 * <dt><em>Schema</em></dt>
 * <dd>A container of tables, views and constraints.
 * </dd>
 * <dt><em>Table</em></dt>
 * <dd>A container of columns and indexes.
 * </dd>
 * </dl>
 * <h1 id="object-identifiers">Object Identifiers</h1>
 * <p>In DBTune, an object is identified by its fully qualified name. When an object is added to its container, there's an optional duplicate detection based on the contents of the element being added to the metadata. For more information on how the duplicate detection mechanism works, check the documentation for the DatabaseObject class.</p>
 * <h1 id="getting-a-dbms-terminology-through-jdbc">Getting a DBMS terminology through JDBC</h1>
 * <p>The JDBC framework defines a class (<a href="http://download.oracle.com/javase/6/docs/api/index.html?java/sql/DatabaseMetaData.html"><code>DatabaseMetaData</code></a>) that, if implemented by a DBMS vendor, can be used to retrieve metadata, including the terminology used by a DBMS.</p>
 * <pre><code>Connection       con        = getConnection();
 * DatabaseMetaData dbMetaData = con.getMetaData();
 * 
 * if (dbMetaData == null) {
 *    throw new SQLException(&quot;Metadata through JDBC not supported&quot;);
 * }
 * 
 * dbMetaData.getCatalogTerm(); // retrieves the DBMS term for Catalog
 * dbMetaData.getSchemaTerm(); // retrieves the DBMS term for Schema
 * </code></pre>
 * <p>The above code should work with any JDBC-complient driver, but note that some implementations don't provide functionality for all the methods of the <code>DatabaseMetaData</code> class.</p>
 * <p>The following maps the terms <em>Catalog</em> and <em>Schema</em> as defined by the SQL standard to how they're used by some DBMSs (obtained through the use of JDBC):</p>
 * <table>
 * <thead>
 * <tr class="header">
 * <th align="center">System</th>
 * <th align="center">Catalog</th>
 * <th align="center">Schema</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr class="odd">
 * <td align="center">PostgreSQL</td>
 * <td align="center">Database</td>
 * <td align="center">Schema</td>
 * </tr>
 * <tr class="even">
 * <td align="center">Oracle</td>
 * <td align="center">N/A</td>
 * <td align="center">User</td>
 * </tr>
 * <tr class="odd">
 * <td align="center">MySQL</td>
 * <td align="center">Database</td>
 * <td align="center">N/A</td>
 * </tr>
 * <tr class="even">
 * <td align="center">Firebird</td>
 * <td align="center">N/A</td>
 * <td align="center">N/A</td>
 * </tr>
 * <tr class="odd">
 * <td align="center">SQLServer</td>
 * <td align="center">Database</td>
 * <td align="center">User</td>
 * </tr>
 * <tr class="even">
 * <td align="center">DB2</td>
 * <td align="center">?</td>
 * <td align="center">?</td>
 * </tr>
 * </tbody>
 * </table>
 * <h1 id="terminology-map-as-viewed-in-dbtune">Terminology Map as viewed in DBTune</h1>
 * <p>The following shows how the terms are given meaning in DBTune for the DBMS that are supported by the API:</p>
 * <table>
 * <thead>
 * <tr class="header">
 * <th align="center">System</th>
 * <th align="center">Catalog</th>
 * <th align="center">Schema</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr class="odd">
 * <td align="center">PostgreSQL</td>
 * <td align="center">Database</td>
 * <td align="center">Schema</td>
 * </tr>
 * <tr class="even">
 * <td align="center">MySQL</td>
 * <td align="center">Server</td>
 * <td align="center">Database</td>
 * </tr>
 * <tr class="odd">
 * <td align="center">DB2</td>
 * <td align="center">?</td>
 * <td align="center">?</td>
 * </tr>
 * </tbody>
 * </table>
 * <p>For <a href="http://postgresql.org">Postgres</a>, the relationship between the terms is the same as the one given by the PostgreSQL JDBC driver. However, for <a href="http://mysql.com">MySQL</a>, we take each database to be a schema and treat a whole MySQL instance as the only Catalog available (called <code>mysql</code> by default).</p>
 */
package edu.ucsc.dbtune.metadata;
