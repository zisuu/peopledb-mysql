package ch.finecloud.peopledb.repository;

import ch.finecloud.peopledb.model.Address;
import ch.finecloud.peopledb.model.Person;
import ch.finecloud.peopledb.model.Region;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
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
    public void canSavePersonWithAddress() throws SQLException {
        Person personAddress = new Person("Peter", "Bmith", ZonedDateTime.of(1980, 11,15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 TestStr", "Apt. 1a", "Bla Bla", "WA", "90210", "United States", "Fulton", Region.WEST);
        personAddress.setHomeAddress(address);

        Person savedPerson = repo.save(personAddress);
        assertThat(savedPerson.getHomeAddress().id()).isGreaterThan(0);
//        connection.commit();
    }

    @Test
    public void canFindPersonByIdWithAddress() throws SQLException {
        Person personAddress = new Person("Peter", "Bmith", ZonedDateTime.of(1980, 11,15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 TestStr", "Apt. 1a", "Bla Bla", "WA", "90210", "United States", "Fulton", Region.WEST);
        personAddress.setHomeAddress(address);

        Person savedPerson = repo.save(personAddress);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().state()).isEqualTo("WA");
//        connection.commit();
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

//    @Test
//    public void loadData() throws IOException, SQLException {
//        Files.lines(Path.of("/Users/Dave/Downloads/Hr5m.csv"))
//                .skip(1)
//                .limit(100)
//                .map(l -> l.split(","))
//                .map(a -> {
//                    LocalDate dob = LocalDate.parse(a[14], DateTimeFormatter.ofPattern("M/d/yyyy"));
//                    LocalTime tob = LocalTime.parse(a[11], DateTimeFormatter.ofPattern("hh:mm:ss a").localizedBy(Locale.ENGLISH));
//                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
//                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
//                    Person person = new Person(a[2], a[4], zdtob);
//                    person.setSalary(new BigDecimal(a[25]));
//                    person.setEmail(a[6]);
//                    return person;
//                })
//                .forEach(repo::save);
////        connection.commit();
//    }

}
