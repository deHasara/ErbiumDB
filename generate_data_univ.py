import random
from faker import Faker

# Function to generate random test data
def generate_test_data(num_persons=20, num_courses=20, num_sections=10, num_students=8,
        num_instructors=8, avg_phone_numbers=10, max_students_per_section=5, max_instructors_per_section=2):
    fake = Faker()

    # Generate Persons
    persons = []
    for i in range(num_persons):
        person = {
            'person_id': i,
            'name': (fake.first_name(), fake.last_name()),
            'street': fake.street_address(),
            'city': fake.city(),
            'phone_numbers': [fake.phone_number() for _ in range(random.randint(1, avg_phone_numbers * 2))]
        }
        persons.append(person)
        print(f"INSERT INTO Person ({i}, {person['name'][0], person['name'][1]}, '{person['street']}', '{person['city']}', {person['phone_numbers']});")

    # Generate Courses
    courses = []
    for i in range(num_courses):
        course = {
            'title': fake.catch_phrase(),
            'credits': str(random.randint(1, 5))
        }
        sql_statement = "INSERT INTO Course VALUES (%s, '%s', '%s');" % (i, course['title'], course['credits'])
        courses.append({'course_id': i, 'title': course['title'], 'credits': course['credits']})
        print(sql_statement)


    # Generate Sections
    sections = []
    existing_sections = set()  # Set to keep track of existing course_id and sec_id combinations

    for _ in range(num_sections):
        while True:
            section = {
                'course_id': random.choice(courses)['course_id'],
                'sec_id': random.randint(1000, 9000),
                'semester': random.choice(['Fall', 'Spring', 'Summer']),
                'year': random.randint(2020, 2024)
            }

            # Check if this course_id and sec_id combination already exists in our local set
            if (section['course_id'], section['sec_id']) not in existing_sections:
                # If it doesn't exist, add it to our set and insert it into the database
                existing_sections.add((section['course_id'], section['sec_id']))
                sections.append(section)

                # Extract values from the section dictionary
                course_id = section['course_id']
                sec_id = section['sec_id']
                semester = section['semester']
                year = section['year']
                
                # Format the SQL statement as a string
                sql_statement = f"INSERT INTO Section VALUES ({course_id}, {sec_id}, '{semester}', {year});"
                
                # Print the SQL statement
                print(sql_statement)
                break  # Exit the while loop and move to the next section

    # Generate Instructors
    instructors = []
    for i in range(len(persons), len(persons) + num_instructors):
        instructor = {
            'person_id': i,
            'name': (fake.first_name(), fake.last_name()),
            'street': fake.street_address(),
            'city': fake.city(),
            'phone_numbers': [fake.phone_number() for _ in range(random.randint(1, avg_phone_numbers * 2))],
            'rank': random.choice(['Assistant', 'Associate', 'Full'])
        }
        persons.append(instructor)
        instructors.append(instructor)
        print(f"INSERT INTO Instructor ({i}, {instructor['name'][0], instructor['name'][1]}, '{instructor['street']}', '{instructor['city']}', {instructor['phone_numbers']}, {instructor['rank']});")

    # Generate Instructors
    students = []
    for i in range(len(persons), len(persons) + num_students):
        student = {
            'person_id': i,
            'name': (fake.first_name(), fake.last_name()),
            'street': fake.street_address(),
            'city': fake.city(),
            'phone_numbers': [fake.phone_number() for _ in range(random.randint(1, avg_phone_numbers * 2))],
            'tot_credits': random.randint(0, 120)
        }
        persons.append(student)
        students.append(student)
        print(f"INSERT INTO Student ({i}, {student['name'][0], student['name'][1]}, '{student['street']}', '{student['city']}', {student['phone_numbers']}, {student['tot_credits']});")

    # Generate Takes
    for section in sections:
        for student in random.sample(students, random.randint(1, max_students_per_section)):
            print(f"INSERT INTO Takes ({student['person_id']}, {section['course_id']}, {section['sec_id']}, '{random.choice(['A', 'B', 'C', 'D', 'F', 'W'])}');")

    # Generate Teaches
    for section in sections:
        for instructor in random.sample(instructors, random.randint(1, max_instructors_per_section)):
            print(f"INSERT INTO Teaches ({instructor['person_id']}, {section['course_id']}, {section['sec_id']});")

    # Generate Advisor
    for student in students:
        print(f"INSERT INTO Advisor ({random.choice(instructors)['person_id']}, {student['person_id']});")

    # Generate Prereq
    for course in courses:
        for prereq in random.sample(courses, random.randint(0, 3)):
            if course['course_id'] != prereq['course_id']:
                print(f"INSERT INTO Prereq ({course['course_id']}, {prereq['course_id']});") 

# Generate test data
generate_test_data()
