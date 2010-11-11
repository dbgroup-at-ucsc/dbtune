/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */
package edu.ucsc.satuning.db;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface DatabaseIndexExtractorFactory<I extends DBIndex<I>> {
    /**
     * makes a new {@link DatabaseIndexExtractor} object.
     * @param connection
     *      the {@link DatabaseConnection} that gets this {@code extractor} assigned to.
     * @return
     *      a dbms-specific index extractor.
     */
    DatabaseIndexExtractor<I> makeIndexExtractor(DatabaseConnection<I> connection);
}
