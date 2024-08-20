import psycopg2
import time
import statistics

# Database connection parameters
db_params = {
    'dbname': 'erexpts1',
    'user': 'amol',
    'host': 'localhost',
    'port': '5432'
}

W = 10

# Queries to compare
expts = [
    # moving a multivalued attribute to a separate relation
    [
        "select person_id, phone_numbers from person;",
        "select x.person_id, array_agg(y.phone_number) from person_alt2 x left join personphone_alt2 y on (x.person_id = y.person_id) group by x.person_id;"
    ],
    [
        "SELECT person_id, ph.phone_number FROM person, LATERAL unnest(person.phone_numbers) AS ph(phone_number);",
        "select person_id, phone_number from personphone_alt2"
    ],
    [
        "select person_id, phone_numbers from person;",
        "select x.person_id, array_agg(y.phone_number) from person_alt2 x left join personphone_alt2 y on (x.person_id = y.person_id) group by x.person_id;"
    ],
    [
        "select unnest(phone_numbers) from person where person_id = 1",
        "select phone_number from personphone_alt2 where person_id = 1"
    ],
    [
        "select unnest(phone_numbers) from person",
        "select phone_number from personphone_alt2"
    ],
    # combining subclasses into a single relation
    [
        "select p.person_id, name, rank from person p join instructor i on p.person_id = i.person_id;",
        "select person_id, name, rank from person_alt1 p where p.rank is not null;"
    ],
    # disjoint relations requires unions 
    [
        "select person_id from person p where (name).first_name = 'Robert'",
        "select person_id from person_alt4 p where (name).first_name = 'Robert' union select person_id from instructor_alt4 p where (name).first_name = 'Robert' union select person_id from student_alt4 p where (name).first_name = 'Robert'"
    ],
    # sections is folded into courses -- impact on finding all courses and their sections
    [
        "select c.course_id, array_agg(s.sec_id) from course c join section s on c.course_id = s.course_id group by c.course_id;",
        "SELECT course_id, ARRAY_AGG(section.sec_id) AS sec_ids FROM Course_alt3, UNNEST(sections) AS section GROUP BY course_id;"
    ],
    # sections is folded into courses -- impact on a join on section primary key
    [
        "select t.person_id from takes t join section s on t.course_id = s.course_id and t.section_id = s.sec_id where s.year = 2024;",
        "SELECT t.person_id FROM Course_alt3, UNNEST(sections) AS section, takes t where t.section_id = section.sec_id and section.year = 2024"
    ],
    # sections is folded into courses -- impact on finding all information about a single course
    [
        "select * from course c join section s on c.course_id = s.course_id where c.course_id = 215",
        "select * from course_alt3 c where course_id = 215",
    ],
    [
        "select course_id, name from section_instructor_alt5",
        "select s.course_id, i.name FROM (Section s JOIN Teaches t ON s.course_id = t.course_id AND s.sec_id = t.section_id) JOIN Person i ON t.person_id = i.person_id;"
    ]
]

# Number of times to run each query
num_runs = 10

def run_query(cursor, query):
    start_time = time.time()
    cursor.execute(query)
    end_time = time.time()
    return end_time - start_time

def compare_queries(query1, query2, num_runs):
    times1 = []
    times2 = []

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        print(f"Running each query {num_runs} times...")

        for i in range(num_runs):
            # Run and time query1
            time1 = run_query(cursor, query1)
            times1.append(time1)

            # Run and time query2
            time2 = run_query(cursor, query2)
            times2.append(time2)

            print(f"Run {i+1}: Query 1 - {time1:.4f}s, Query 2 - {time2:.4f}s")

        # Calculate statistics
        avg1 = statistics.mean(times1)
        avg2 = statistics.mean(times2)
        median1 = statistics.median(times1)
        median2 = statistics.median(times2)
        stdev1 = statistics.stdev(times1)
        stdev2 = statistics.stdev(times2)

        print("\nResults:")
        print(f"Query 1 - Avg: {avg1:.4f}s, Median: {median1:.4f}s, StdDev: {stdev1:.4f}s")
        print(f"Query 2 - Avg: {avg2:.4f}s, Median: {median2:.4f}s, StdDev: {stdev2:.4f}s")

        if avg1 < avg2:
            print(f"\nQuery 1 is faster by {(avg2-avg1)/avg1*100:.2f}%")
        else:
            print(f"\nQuery 2 is faster by {(avg1-avg2)/avg2*100:.2f}%")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if conn:
            cursor.close()
            conn.close()

# Run the comparison
print("Comparing ---")
print(expts[W][0])
print(expts[W][1])
print("------------------------")
compare_queries(expts[W][0], expts[W][1], num_runs)
