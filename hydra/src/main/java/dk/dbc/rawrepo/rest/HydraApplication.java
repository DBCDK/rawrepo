/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.rest;

import dk.dbc.rawrepo.LibraryAPI;
import dk.dbc.rawrepo.QueueAPI;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class defines the other classes that make up this JAX-RS application
 * by having the getClasses method return a specific set of resources.
 */
@ApplicationPath("/api")
public class HydraApplication extends Application {
    private static final Set<Class<?>> classes = new HashSet<>(Arrays.asList(QueueAPI.class, LibraryAPI.class));

    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }
}