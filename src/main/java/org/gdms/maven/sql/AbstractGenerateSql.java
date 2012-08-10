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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import com.pyx4j.log4j.MavenLogAppender;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import org.gdms.sql.engine.Engine;
import org.gdms.sql.engine.SQLScript;

/**
 * Base class for sqlCompile and testSqlCompile goals.
 *
 * @author Antoine Gourlay
 */
abstract class AbstractGenerateSql extends AbstractMojo {

        protected void doExecute(File sqlScriptsDirectory, File outputDirectory) throws MojoExecutionException, MojoFailureException {
                MavenLogAppender.startPluginLog(this);

                try {
                        final String inputPath = sqlScriptsDirectory.getAbsolutePath();

                        getLog().info(String.format("Processing folder '%s'.", inputPath));
                        if (!sqlScriptsDirectory.exists()) {
                                // nothing to do, no valid input directory
                                getLog().warn("Directory does not exist! Nothing to do.");
                                return;
                        }

                        Collection<File> fil = FileUtils.listFiles(sqlScriptsDirectory, new String[]{"sql"}, true);

                        int size = fil.size();
                        if (size == 0) {
                                // nothing to do, no files...
                                getLog().warn("Found 0 sql files! Nothing to do.");
                        } else {
                                getLog().info(String.format("Compiling %d sql files to '%s'",
                                        size, outputDirectory.getAbsolutePath()));

                                // be sure the output dir exists
                                if (!outputDirectory.exists()) {
                                        outputDirectory.mkdirs();
                                }
                                
                                boolean errors = false;

                                for (File ff : fil) {
                                        SQLScript s;
                                        try {
                                                s = Engine.parseScript(ff);
                                        } catch (Exception e) {
                                                // error reporting could be improved here...
                                                errors = true;
                                                getLog().error(String.format(
                                                        "Failed to compile script '%s':", ff.getAbsolutePath()));
                                                getLog().error(e.getLocalizedMessage());
                                                getLog().debug(e);
                                                continue;
                                        }

                                        String oldpath = ff.getParentFile().getAbsolutePath();
                                        String localpath = oldpath.substring(inputPath.length());

                                        File targetDir = new File(outputDirectory, localpath);
                                        targetDir.mkdirs();

                                        final String fName = ff.getName();
                                        File targetFile = new File(targetDir, fName.substring(0, fName.length() - 3) + "bsql");

                                        // delete existing compiled script
                                        // maybe we could check if the input script changed before doing anything...
                                        if (targetFile.exists()) {
                                                targetFile.delete();
                                        }

                                        try {
                                                s.save(new FileOutputStream(targetFile));
                                        } catch (IOException ex) {
                                                // this is not good, let's abort
                                                throw new MojoExecutionException(String.format(
                                                        "Error while saving to '%s'", targetFile.getAbsolutePath()), ex);
                                        }
                                }
                                
                                if (errors) {
                                        throw new MojoFailureException(
                                                "There were build errors! See above for more details.");
                                }
                        }
                } finally {
                        MavenLogAppender.endPluginLog(this);
                }
        }
}
