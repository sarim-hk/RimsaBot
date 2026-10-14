import inspect
import json
from typing import get_args, get_type_hints, Any
from utils.database import Database

def convert(value: str, annotation: Any):
    value = value.strip()

    if value == "" and type(None) in get_args(annotation):
        return None

    if annotation is int:
        return int(value)

    return value

def main():
    with open("config.json") as file:
        config = json.load(file)

    db = Database(config)

    functions = {
        name: method
        for name, method in inspect.getmembers(db, predicate=callable)
        if not name.startswith("_")
    }

    while True:
        print("\nAvailable functions:")

        names = list(functions)

        for index, name in enumerate(names, start=1):
            print(f"{index}. {name}")

        print("q. Quit")

        choice = input("\nChoose a function: ")

        if choice.lower() == "q":
            break

        if not choice.isdigit() or not 1 <= int(choice) <= len(names):
            print("Invalid choice.")
            continue

        function_name = names[int(choice) - 1]
        function = functions[function_name]
        signature = inspect.signature(function)
        hints = get_type_hints(function)

        arguments = {}

        for parameter in signature.parameters.values():
            annotation = hints.get(parameter.name, str)
            value = input(f"{parameter.name} ({annotation.__name__}): ")
            arguments[parameter.name] = convert(value, annotation)

        try:
            result = function(**arguments)
            print(f"\nResult: {result}")
        except Exception as error:
            print(f"\nError: {type(error).__name__}: {error}")

if __name__ == "__main__":
    main()