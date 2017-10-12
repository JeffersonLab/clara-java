# CLARA

A service-oriented framework to provide a computing environment for efficient
Big Data processing.

[![Build Status](https://travis-ci.org/JeffersonLab/clara-java.svg?branch=master)](https://travis-ci.org/JeffersonLab/clara-java)
[![Javadoc](https://img.shields.io/badge/javadoc-4.3--SNAPSHOT-blue.svg?style=flat)](https://claraweb.jlab.org/clara/api/java/v4.3)


## Documentation

The reference documentation is available at <http://claraweb.jlab.org>.


## Build notes

CLARA requires the Java 8 JDK.

#### Ubuntu

Support PPAs:

    $ sudo apt-get install software-properties-common

Install Oracle Java 8 from the
[Web Upd8 PPA](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html):

    $ sudo add-apt-repository ppa:webupd8team/java
    $ sudo apt-get update
    $ sudo apt-get install oracle-java8-installer

Check the version:

    $ java -version
    java version "1.8.0_101"
    Java(TM) SE Runtime Environment (build 1.8.0_101-b13)
    Java HotSpot(TM) 64-Bit Server VM (build 25.101-b13, mixed mode)

You may need the following package to set Java 8 as default
(see the previous link for more details):

    $ sudo apt-get install oracle-java8-set-default

You can also set the default Java version with `update-alternatives`:

    $ sudo update-alternatives --config java

#### macOS

Install Oracle Java using [Homebrew](https://brew.sh/):

    $ brew cask install java

Check the version:

    $ java -version
    java version "1.8.0_92"
    Java(TM) SE Runtime Environment (build 1.8.0_92-b14)
    Java HotSpot(TM) 64-Bit Server VM (build 25.92-b14, mixed mode)


### Installation

To build CLARA use the provided [Gradle](https://gradle.org/) wrapper.
It will download the required Gradle version and all the CLARA dependencies.

    $ ./gradlew

To install the CLARA artifact to the local Maven repository:

    $ ./gradlew install

To deploy the binary distribution to `$CLARA_HOME`:

    $ ./gradlew deploy


### Importing the project into your IDE

Gradle can generate the required configuration files to import the CLARA
project into [Eclipse](https://eclipse.org/ide/) and
[IntelliJ IDEA](https://www.jetbrains.com/idea/):

    $ ./gradlew cleanEclipse eclipse

    $ ./gradlew cleanIdea idea

See also the [Eclipse Buildship plugin](http://www.vogella.com/tutorials/EclipseGradle/article.html)
and the [Intellij IDEA Gradle Help](https://www.jetbrains.com/help/idea/2016.2/gradle.html).


## Authors

For assistance contact authors:

* Vardan Gyurjyan    (<gurjyan@jlab.org>)
* Sebastián Mancilla (<smancill@jlab.org>)
* Ricardo Oyarzún    (<oyarzun@jlab.org>)
