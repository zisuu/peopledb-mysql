package ch.finecloud.peopledb.model;

import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PersonTest {

    @Test
    public void testForEquality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(2000,12,25,13,41,14,0, ZoneId.of("+0")));
        Person p2 = new Person("p1", "smith", ZonedDateTime.of(2000,12,25,13,41,14,0, ZoneId.of("+0")));
        assertThat(p1).isEqualTo(p2);
    }

    @Test
    public void testForInequality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(2000,12,25,13,41,14,0, ZoneId.of("+0")));
        Person p2 = new Person("p2", "smith", ZonedDateTime.of(2000,12,25,13,41,14,0, ZoneId.of("+0")));
        assertThat(p1).isNotEqualTo(p2);
    }
}