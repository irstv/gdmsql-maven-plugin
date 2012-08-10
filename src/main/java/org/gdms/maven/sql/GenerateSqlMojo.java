/**
 * The GDMS library (Generic Datasource Management System)
 * is a middleware dedicated to the management of various kinds of
 * data-sources such as spatial vectorial data or alphanumeric. Based
 * on the JTS library and conform to the OGC simple feature access
 * specifications, it provides a complete and robust API to manipulate
 * in a SQL way remote DBMS (PostgreSQL, H2...) or flat files (.shp,
 * .csv...).
 *
 * Gdms is distributed under GPL 3 license. It is produced by the "Atelier SIG"
 * team of the IRSTV Institute <http://www.irstv.fr/> CNRS FR 2488.
 *
 * Copyright (C) 2007-2012 IRSTV FR CNRS 2488
 *
 * This file is part of Gdms.
 *
 * Gdms is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Gdms is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Gdms. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 *
 * or contact directly:
 * info@orbisgis.org
 */
package org.gdms.maven.sql;

import java.io.File;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Goal which compiles Gdms sql script into binary form.
 *
 * @goal sqlCompile
 * @phase compile
 * @author Antoine Gourlay
 */
public class GenerateSqlMojo extends AbstractGenerateSql {

        /**
         * Location of the output class directory.
         * @parameter expression="${project.build.outputDirectory}"
         * @required
         */
        private File outputDirectory;
        /**
         * Location of the input script files.
         * @parameter expression="${basedir}${file.separator}src${file.separator}main${file.separator}sql"
         * @required
         */
        private File sqlScriptsDirectory;
        
        public void execute() throws MojoExecutionException, MojoFailureException {
                doExecute(sqlScriptsDirectory, outputDirectory);
        }
}
