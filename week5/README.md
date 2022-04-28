Discussion Question:

In the lecture we mentioned the benefits of Data Transformation, but can you think of any
problems that might arise with Data Transformation?
Do you think data transformation or validation should come first in your pipeline? Why or why
not?

Group Response:
What if you had logic to remove outliers, but then your assumptions are wrong.
I think that validation should come first. Transforming is a more intense operation,
and validation before transformation means that fewer transformations would be needed.
Most validation that I am experienced with is web form data where sometimes keystrokes are
limited. Limiting input, allows for easier and more predictable transforms.

My Response:
It is very possible that you could be injecting your own biases into the data.
Validation should absolutely come first.


ETL (Extract, Transform, Load) is a common pipeline process. Describe the delineation of each of these separate activities. For example, how is extraction different from transformation

Group Response:
Is validation a part of extraction or transformation? You'd almost want to do data validation before doing extraction. So perhaps it is better to say "Validate, Extract, Transform, Load"

My Response:
During extraction, you're only selecting which columns you want. Transformation is a purposeful mutation of the data to meet whatever your goal is.