package org.jlab.clara.base;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClaraFiltersTest {

    @Test
    public void allDpes() throws Exception {
        ClaraFilter f = ClaraFilters.allDpes();

        assertThat(f.filter(), is("name=*"));
        assertThat(f.type(),   is("dpe"));
    }


    @Test
    public void allContainers() throws Exception {
        ClaraFilter f = ClaraFilters.allContainers();

        assertThat(f.filter(), is("name=*"));
        assertThat(f.type(),   is("container"));
    }


    @Test
    public void allServices() throws Exception {
        ClaraFilter f = ClaraFilters.allServices();

        assertThat(f.filter(), is("name=*"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void dpesByLanguage() throws Exception {
        ClaraFilter f = ClaraFilters.dpesByLanguage(ClaraLang.JAVA);

        assertThat(f.filter(), is("lang=java"));
        assertThat(f.type(),   is("dpe"));
    }


    @Test
    public void containersByHost() throws Exception {
        ClaraFilter f = ClaraFilters.containersByHost("10.1.1.2");

        assertThat(f.filter(), is("host=10.1.1.2"));
        assertThat(f.type(),   is("container"));
    }


    @Test
    public void containersByDpe() throws Exception {
        ClaraFilter f = ClaraFilters.containersByDpe("10.1.1.2_java");

        assertThat(f.filter(), is("dpe=10.1.1.2_java"));
        assertThat(f.type(),   is("container"));
    }


    @Test
    public void containersByLanguage() throws Exception {
        ClaraFilter f = ClaraFilters.containersByLanguage(ClaraLang.JAVA);

        assertThat(f.filter(), is("lang=java"));
        assertThat(f.type(),   is("container"));
    }


    @Test
    public void containersByName() throws Exception {
        ClaraFilter f = ClaraFilters.containersByName("master");

        assertThat(f.filter(), is("name=master"));
        assertThat(f.type(),   is("container"));
    }


    @Test
    public void servicesByHost() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByHost("10.1.1.2");

        assertThat(f.filter(), is("host=10.1.1.2"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void servicesByDpe() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByDpe("10.1.1.2_java");

        assertThat(f.filter(), is("dpe=10.1.1.2_java"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void servicesByContainer() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByContainer("10.1.1.2:master");

        assertThat(f.filter(), is("container=10.1.1.2:master"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void servicesByLanguage() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByLanguage(ClaraLang.CPP);

        assertThat(f.filter(), is("lang=cpp"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void servicesByName() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByName("master");

        assertThat(f.filter(), is("name=master"));
        assertThat(f.type(),   is("service"));
    }


    @Test
    public void servicesByAuthor() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByAuthor("Clint Eastwood");

        assertThat(f.filter(), is("author=Clint Eastwood"));
        assertThat(f.type(),   is("service"));
    }



    @Test
    public void servicesByDescription() throws Exception {
        ClaraFilter f = ClaraFilters.servicesByDescription("metal*");

        assertThat(f.filter(), is("description=metal*"));
        assertThat(f.type(),   is("service"));
    }
}
