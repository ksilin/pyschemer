from dataclasses import dataclass, field


@dataclass
class PersonAddedJob:
    firstName: str = field(metadata={"description": "The person's first name."})
    lastName: str = field(metadata={"description": "The person's last name."})
    job: str = field(metadata={"description": "The person's job."})
    age: int = field(default=0, metadata={"description": "Age in years which must be equal to or greater than zero.", "minimum": 0})

    def __post_init__(self):
        if not self.firstName or not self.lastName:
            raise ValueError("firstName and lastName are required fields.")
        if self.age < 0:
            raise ValueError("age must be equal to or greater than zero.")

