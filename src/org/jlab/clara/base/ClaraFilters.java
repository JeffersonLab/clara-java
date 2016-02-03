/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.base;

/**
 * The standard filters to select Clara DPEs, containers or services.
 */
public final class ClaraFilters {

    static final String TYPE_DPE = "dpe";
    static final String TYPE_CONTAINER = "container";
    static final String TYPE_SERVICE = "service";

    private ClaraFilters() {
    }

    /**
     * Returns a filter to select all DPEs in the Clara cloud.
     * The filter will select every running DPE, of any language.
     */
    public static ClaraFilter allDpes() {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "name=*";
            }

            @Override
            String type() {
                return TYPE_DPE;
            }
        };
    }


    /**
     * Returns a filter to select all containers in the Clara cloud.
     * The filter will select every deployed container, in every DPE, of any language.
     */
    public static ClaraFilter allContainers() {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "name=*";
            }

            @Override
            String type() {
                return TYPE_CONTAINER;
            }
        };
    }


    /**
     * Returns a filter to select all services in the Clara cloud.
     * The filter will select every deployed service, in every container of every DPE,
     * of any language.
     */
    public static ClaraFilter allServices() {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "name=*";
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the DPEs of the given language.
     * The filter will select every running DPE of the specified language.
     *
     * @param lang the language to filter
     */
    public static ClaraFilter dpesByLanguage(final String lang) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "lang=" + lang;
            }

            @Override
            String type() {
                return TYPE_DPE;
            }
        };
    }


    /**
     * Returns a filter to select all the containers of the given host.
     * A host can contain multiple DPEs of different languages.
     * The filter will select all containers deployed on every DPE running
     * in the specified host.
     * <p>
     * Example: all the containers on the host {@code 10.2.9.100}.
     *
     * @param host the selected host
     */
    public static ClaraFilter containersByHost(final String host) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "host=" + host;
            }

            @Override
            String type() {
                return TYPE_CONTAINER;
            }
        };
    }


    /**
     * Returns a filter to select all the containers of the given DPE.
     * A host can contain multiple DPEs of different languages.
     * The filter will select every container deployed on the specified DPE.
     * <p>
     * Example: all the containers on the DPE {@code 10.2.9.100_cpp}.
     *
     * @param dpeName the selected DPE
     */
    public static ClaraFilter containersByDpe(final String dpeName) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "dpe=" + dpeName;
            }

            @Override
            String type() {
                return TYPE_CONTAINER;
            }
        };
    }


    /**
     * Returns a filter to select all the containers of the given language.
     * The filter will select all containers deployed on every running DPE of
     * the specified language.
     * <p>
     * Example: all the {@code java} containers.
     *
     * @param lang the language to filter
     */
    public static ClaraFilter containersByLanguage(final String lang) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "lang=" + lang;
            }

            @Override
            String type() {
                return TYPE_CONTAINER;
            }
        };
    }


    /**
     * Returns a filter to select all the containers of the given name.
     * The filter will select every container deployed on any running DPE whose
     * name matches the specified name. The match must be exact.
     * <p>
     * Example: all containers named {@code master}.
     *
     * @param name the container name to filter
     */
    public static ClaraFilter containersByName(final String name) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "name=" + name;
            }

            @Override
            String type() {
                return TYPE_CONTAINER;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given host.
     * A host can contain multiple DPEs of different languages.
     * The filter will select all services deployed on every DPE running
     * in the specified host.
     * <p>
     * Example: all the services on the host {@code 10.2.9.100}.
     *
     * @param host the selected host
     */
    public static ClaraFilter servicesByHost(final String host) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "host=" + host;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given DPE.
     * A host can contain multiple DPEs of different languages.
     * The filter will select every service deployed on the specified DPE.
     * <p>
     * Example: all the services on the DPE {@code 10.2.9.100_cpp}.
     *
     * @param dpeName the selected DPE
     */
    public static ClaraFilter servicesByDpe(final String dpeName) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "dpe=" + dpeName;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given container.
     * The filter will select every service deployed on the specified container.
     * <p>
     * Example: all the services on the container {@code 10.2.9.100_cpp:master}.
     *
     * @param containerName the selected container
     */
    public static ClaraFilter servicesByContainer(final String containerName) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "container=" + containerName;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given language.
     * The filter will select all services deployed on every running DPE of
     * the specified language.
     * <p>
     * Example: all the {@code java} services.
     *
     * @param lang the language to filter
     */
    public static ClaraFilter servicesByLanguage(final String lang) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "lang=" + lang;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given name.
     * The filter will select every service deployed on any running DPE whose
     * engine matches the specified name. The match must be exact.
     * <p>
     * Example: all services named {@code SqrRoot}.
     *
     * @param name the engine name to filter
     */
    public static ClaraFilter servicesByName(final String name) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "name=" + name;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given author.
     * The filter will select every service deployed on any running DPE whose
     * author matches the specified name. The match must be exact.
     * <p>
     * Example: all services developed by {@code John Doe}.
     *
     * @param authorName the author name to filter
     */
    public static ClaraFilter servicesByAuthor(final String authorName) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "author=" + authorName;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }


    /**
     * Returns a filter to select all the services of the given description.
     * The filter will select every service deployed on any running DPE whose
     * description matches the specified regular expression.
     * <p>
     * Example: all services with the regex {@code stat*} in its description.
     *
     * @param regex the engine name to filter
     */
    public static ClaraFilter servicesByDescription(final String regex) {
        return new ClaraFilter() {
            @Override
            String filter() {
                return "description=" + regex;
            }

            @Override
            String type() {
                return TYPE_SERVICE;
            }
        };
    }
}
