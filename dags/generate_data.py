import pandas as pd
import json
import random
from datetime import datetime
import os

def ensure_data_directory():
    """Создает папку data, если она не существует"""
    data_dir = 'dags/data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f" Создана папка {data_dir}")
    return data_dir

def generate_employees_data(data_dir):
    """Генерация данных о сотрудниках (CSV)"""
    print("Генерация данных о сотрудниках...")
    
    departments = ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Operations', 'R&D']
    
    employees_data = {
        'employee_id': list(range(1, 101)),  # 100 сотрудников
        'department': [random.choice(departments) for _ in range(100)]
    }
    
    df = pd.DataFrame(employees_data)
    file_path = os.path.join(data_dir, 'employees.csv')
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f" Создан файл {file_path} с {len(df)} записями")
    
    return df

def generate_courses_data(data_dir):
    """Генерация данных о курсах (JSON)"""
    print("Генерация данных о курсах...")
    
    courses = [
        "Python Programming",
        "Data Analysis", 
        "Machine Learning",
        "Web Development",
        "Database Management",
        "Project Management",
        "Business Analytics",
        "Cloud Computing",
        "Cybersecurity",
        "DevOps Fundamentals"
    ]
    
    courses_data = [
        {'course_id': i + 1, 'course_name': course}
        for i, course in enumerate(courses)
    ]
    
    file_path = os.path.join(data_dir, 'courses.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(courses_data, f, indent=2, ensure_ascii=False)
    
    print(f" Создан файл {file_path} с {len(courses_data)} курсами")
    return courses_data

def generate_training_data(employees_df, courses_data, data_dir):
    """Генерация данных об обучении (Excel)"""
    print("Генерация данных об обучении...")
    
    training_records = []
    
    for employee_id in employees_df['employee_id']:
        # Каждый сотрудник проходит от 1 до 4 курсов
        num_courses = random.randint(1, 4)
        taken_courses = random.sample(courses_data, num_courses)
        
        for course in taken_courses:
            score = random.randint(60, 100)  # Оценка от 60 до 100
            training_records.append({
                'employee_id': employee_id,
                'course_id': course['course_id'],
                'score': score
            })
    
    training_df = pd.DataFrame(training_records)
    file_path = os.path.join(data_dir, 'training.xlsx')
    training_df.to_excel(file_path, index=False, engine='openpyxl')
    print(f" Создан файл {file_path} с {len(training_df)} записями об обучении")
    
    return training_df

def generate_statistics(employees_df, courses_data, training_df, data_dir):
    """Генерация статистики по данным"""
    print("\n📊 СТАТИСТИКА СГЕНЕРИРОВАННЫХ ДАННЫХ:")
    print("=" * 50)
    
    # Статистика по сотрудникам
    dept_stats = employees_df['department'].value_counts()
    print(f" Всего сотрудников: {len(employees_df)}")
    print("Распределение по отделам:")
    for dept, count in dept_stats.items():
        print(f"  - {dept}: {count} сотрудников")
    
    # Статистика по курсам
    print(f"\n Всего курсов: {len(courses_data)}")
    print("Список курсов:")
    for course in courses_data:
        print(f"  - {course['course_id']}: {course['course_name']}")
    
    # Статистика по обучению
    print(f"\n Всего записей об обучении: {len(training_df)}")
    print(f"Среднее количество курсов на сотрудника: {len(training_df) / len(employees_df):.1f}")
    
    # Статистика по оценкам
    avg_score = training_df['score'].mean()
    max_score = training_df['score'].max()
    min_score = training_df['score'].min()
    print(f" Статистика оценок:")
    print(f"  - Средняя оценка: {avg_score:.1f}")
    print(f"  - Максимальная оценка: {max_score}")
    print(f"  - Минимальная оценка: {min_score}")
    
    # Популярность курсов
    course_popularity = training_df['course_id'].value_counts().sort_index()
    print(f"\n Популярность курсов:")
    for course_id, count in course_popularity.items():
        course_name = next(c['course_name'] for c in courses_data if c['course_id'] == course_id)
        print(f"  - {course_name}: {count} сотрудников")
    
    # Сохраняем статистику в файл
    stats_data = {
        'total_employees': len(employees_df),
        'total_courses': len(courses_data),
        'total_training_records': len(training_df),
        'average_courses_per_employee': len(training_df) / len(employees_df),
        'average_score': avg_score,
        'max_score': max_score,
        'min_score': min_score,
        'department_distribution': dept_stats.to_dict(),
        'course_popularity': course_popularity.to_dict()
    }


def main():
    """Основная функция генерации всех данных"""
    print(" ЗАПУСК ГЕНЕРАЦИИ ТЕСТОВЫХ ДАННЫХ")
    print("=" * 60)
    
    try:
        # Создаем папку для данных
        data_dir = ensure_data_directory()
        
        # Генерация всех данных
        employees_df = generate_employees_data(data_dir)
        courses_data = generate_courses_data(data_dir)
        training_df = generate_training_data(employees_df, courses_data, data_dir)
        
        # Показать статистику
        generate_statistics(employees_df, courses_data, training_df, data_dir)
        
        print("\n ВСЕ ДАННЫЕ УСПЕШНО СГЕНЕРИРОВАНЫ!")
        print(f"\n Созданные файлы в папке {data_dir}:")
        print("  - employees.csv (данные о сотрудниках)")
        print("  - courses.json (данные о курсах)") 
        print("  - training.xlsx (данные об обучении)")
  
        
    except Exception as e:
        print(f" Ошибка при генерации данных: {str(e)}")
        raise

if __name__ == "__main__":
    main()