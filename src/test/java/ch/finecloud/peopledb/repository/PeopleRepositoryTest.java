package ch.finecloud.peopledb.repository;

import ch.finecloud.peopledb.model.Person;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;

    @Before
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql://localhost/PEOPLETEST?" +
                "user=root&password=iT8$o^JWZwSTJL");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @After
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Amith", ZonedDateTime.of(1980, 11,15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person person1 = new Person("Peter", "Bmith", ZonedDateTime.of(1980, 11,15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person person2 = new Person("Bobby", "Cmith", ZonedDateTime.of(1992, 9,3, 5, 11, 9, 0, ZoneId.of("+1")));
        Person savedPerson1 = repo.save(person1);
        Person savedPerson2 = repo.save(person2);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindAll() {
        Person savedPerson = repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        repo.save(new Person("Test2", "Person2", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount+2);
    }


    @Test
    public void canDeletePerson() {
        Person savedPerson = repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount-1);
    }

    @Test
    public void canDeletePeople() {
        Person p1 = repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        Person p2 = repo.save(new Person("Test", "Person", ZonedDateTime.of(2022, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        repo.delete(p1, p2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("Peter", "Mueller", ZonedDateTime.of(2001, 11, 25, 18, 55, 17, 0, ZoneId.of("+0"))));
        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("7300.00"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();
        assertThat(p2.getSalary()).isNotEqualTo(p1.getSalary());
    }

}
