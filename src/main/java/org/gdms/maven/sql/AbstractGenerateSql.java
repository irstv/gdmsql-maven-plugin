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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.pyx4j.log4j.MavenLogAppender;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;

import org.gdms.data.DataSourceFactory;
import org.gdms.sql.engine.Engine;
import org.gdms.sql.engine.ParseException;
import org.gdms.sql.engine.SQLScript;

/**
 * Base class for sqlCompile and testSqlCompile goals.
 *
 * @author Antoine Gourlay
 */
abstract class AbstractGenerateSql extends AbstractMojo {

        /**
         * Properties to pass to the Gdms SQL Engine.
         * @parameter
         * @optional
         */
        protected Properties engineProperties;

        protected void doExecute(File sqlScriptsDirectory, File outputDirectory) throws MojoExecutionException, MojoFailureException {
                MavenLogAppender.startPluginLog(this);

                try {
                        final String inputPath = sqlScriptsDirectory.getAbsolutePath();

                        getLog().info(String.format("Processing folder %s", inputPath));
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
                                // be sure the output dir exists
                                if (!outputDirectory.exists()) {
                                        outputDirectory.mkdirs();
                                }

                                List<File> changedFiles = new ArrayList<File>();

                                for (File ff : fil) {
                                        File tff = getTargetFile(inputPath, outputDirectory, ff);
                                        if (!tff.exists() || FileUtils.isFileNewer(ff, tff)) {
                                                changedFiles.add(ff);
                                        }
                                }

                                if (changedFiles.isEmpty()) {
                                        getLog().info("Nothing to compile - all compiled scripts are up to date");
                                } else if (changedFiles.size() != size) {
                                        getLog().info(String.format("Compiling %d changed sql files out of %d to %s",
                                                changedFiles.size(), size, outputDirectory.getAbsolutePath()));
                                } else {
                                        getLog().info(String.format("Compiling %d sql files to %s",
                                                size, outputDirectory.getAbsolutePath()));
                                }

                                Properties props = DataSourceFactory.getDefaultProperties();

                                if (engineProperties != null) {
                                        props = new Properties(props);
                                        for (String k : engineProperties.stringPropertyNames()) {
                                                props.setProperty(k, engineProperties.getProperty(k));
                                        }
                                }

                                if (getLog().isDebugEnabled()) {
                                        // display properties
                                        Log log = getLog();
                                        log.debug("Engine invocation properties:");
                                        if (engineProperties == null) {
                                                log.debug("No custom properties. Using Default:");
                                        } else {
                                                log.debug("Custom properties:");
                                                printProperties(engineProperties);
                                                log.debug("Merged with default properties:");
                                        }
                                        printProperties(props);
                                }

                                int errors = 0;

                                for (File ff : changedFiles) {
                                        if (getLog().isDebugEnabled()) {
                                                getLog().debug("Parsing script " + ff.getAbsolutePath());
                                        }
                                        SQLScript s;
                                        try {
                                                s = Engine.parseScript(ff, props);
                                        } catch (Exception e) {
                                                if (errors == 0) {
                                                        getLog().info("---------------------------------------");
                                                        getLog().error("COMPILATION ERROR :");
                                                        getLog().info("---------------------------------------");
                                                }
                                                errors++;
                                                getLog().error(e.getLocalizedMessage());
                                                getLog().info(String.format(
                                                        "location:  %s", ff.getAbsolutePath()));
                                                if (e instanceof ParseException) {
                                                        ParseException p = (ParseException) e;
                                                        getLog().info(p.getLocation().prettyPrint());
                                                }
                                                getLog().debug(e);
                                                getLog().info("---------------------------------------");
                                                continue;
                                        }
                                        File targetFile = getTargetFile(inputPath, outputDirectory, ff);

                                        // create parent directory. Might not exist
                                        targetFile.getParentFile().mkdirs();

                                        // delete existing compiled script
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

                                if (errors != 0) {
                                        getLog().info(String.format("%d errors", errors));
                                        String errorStr;
                                        if (errors == 1) {
                                                errorStr = "There was 1 SQL build error!";
                                        } else {
                                                errorStr = String.format("There were %d SQL build errors!", errors);
                                        }
                                        throw new MojoFailureException(errorStr + " See above for more details.");
                                }
                        }
                } finally {
                        MavenLogAppender.endPluginLog(this);
                }
        }

        private File getTargetFile(String inputPath, File outputDirectory, File sourceFile) {
                String oldpath = sourceFile.getParentFile().getAbsolutePath();
                String localpath = oldpath.substring(inputPath.length());

                File targetDir = new File(outputDirectory, localpath);

                final String fName = sourceFile.getName();

                return new File(targetDir, fName.substring(0, fName.length() - 3) + "bsql");
        }

        private void printProperties(Properties p) {
                Log log = getLog();
                log.debug(" {");
                for (String key : p.stringPropertyNames()) {
                        log.debug("  " + key + " = " + p.getProperty(key));
                }
                log.debug(" }");
        }
}
