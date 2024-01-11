# Methods of Advanced Data Engineering Template Project

This template project provides some structure for your open data project in the MADE module at FAU.
This repository contains (a) a data science project that is developed by the student over the course of the semester, and (b) the exercises that are submitted over the course of the semester.
Before you begin, make sure you have [Python](https://www.python.org/) and [Jayvee](https://github.com/jvalue/jayvee) installed. We will work with [Jupyter notebooks](https://jupyter.org/). The easiest way to do so is to set up [VSCode](https://code.visualstudio.com/) with the [Jupyter extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter).


## Project Work
In a city where biking is a way of life, understanding the impact of weather on cycling habits is crucial. I have focused on unraveling the complex relationship between weather conditions and bike usage in Munich. This endeavor was aimed at enhancing urban mobility strategies and promoting environmental sustainability through increased bicycle usage.Throughout this project, I have delved into the following key areas:

Analysis of Weather Patterns and Biking Trends in Munich (2020).

1. Investigating how different weather conditions throughout the year influenced cycling habits ?.
2. Understanding weekly variations in bike usage in relation to weather changes ?.
3. How does the frequency of biking change during specific seasons in 2020?
4. How Cyclists and Weather has been correlated to each other ?.
5. How does the average temperature influence bike usage in Munich on a monthly basis?
   
I invite you to explore the insights and conclusions drawn from this comprehensive analysis [[1]](https://github.com/hamzanaeem1999/made-template/blob/main/project/report.ipynb). The detailed findings and recommendations are presented in the report, which is an invaluable resource for anyone interested in urban mobility, environmental sustainability, and the promotion of cycling in urban settings.


### Exporting a Jupyter Notebook
Jupyter Notebooks can be exported using `nbconvert` (`pip install nbconvert`). For example, to export the example notebook to html: `jupyter nbconvert --to html examples/final-report-example.ipynb --embed-images --output final-report.html`


## Exercises
During the semester you will need to complete exercises, sometimes using [Python](https://www.python.org/), sometimes using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.<jv or py>`.

In regular intervalls, exercises will be given as homework to complete during the semester. We will divide you into two groups, one completing an exercise in Jayvee, the other in Python, switching each exercise. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/). At the end of the semester, you will therefore have the following files in your repository:

1. `./exercises/exercise1.jv` or `./exercises/exercise1.py`
2. `./exercises/exercise2.jv` or `./exercises/exercise2.py`
3. `./exercises/exercise3.jv` or `./exercises/exercise3.py`
4. `./exercises/exercise4.jv` or `./exercises/exercise4.py`
5. `./exercises/exercise5.jv` or `./exercises/exercise5.py`

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

To view your exercise feedback, navigate to Actions -> Exercise Feedback in your repository.

The exercise feedback is executed whenever you make a change in files in the `exercise` folder and push your local changes to the repository on GitHub. To see the feedback, open the latest GitHub Action run, open the `exercise-feedback` job and `Exercise Feedback` step. You should see command line output that contains output like this:

```sh
Found exercises/exercise1.jv, executing model...
Found output file airports.sqlite, grading...
Grading Exercise 1
	Overall points 17 of 17
	---
	By category:
		Shape: 4 of 4
		Types: 13 of 13
```
