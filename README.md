# Introduction to Databases 23500 / 33550
This is the main repository for Intro to DB. Homework assignments, project files, and extra class materials will come to this repository in a branch (e.g. "hw3" or "lab2").
Your homework and project submissions will be made here, committed in a specified branch, pushed to your remote repository on GitLab, and then submitted via [Chisubmit](http://chi.cs.uchicago.edu/chisubmit/).
If you are not familiar with Git, I strongly suggest you take 15 minutes and visit [TryGit](https://try.github.io/levels/1/challenges/1). 

 - The hws directory is for smaller homeworks, with each homework as a subdirectory. 
 - The simpledb directory is a course long project for people enrolled in 23500 that is a pedagogical database written in Java. Note that some documentation may refer to labs,
these are not labs in CSIL, but a legacy term. You can think of them as milestones with due dates.
 - The coldb directory is a course project for people enrolled in 33550. Here you will write a column-oriented main-memory database in C, nearly from scratch. This project will have milestones, but
the main deliverable will be do at the end of the quarter. There will be little supervision on this project and requires students to be proactive.
 - The materials directory is more any miscellaneous course materials that cannot be distributed via Piazza.
 

course-info
===========

GitLab Repo for U. Chicago 23500/33550


We will be using git, a source code control tool, for labs in 23500.  This
will allow you to download the code for the labs, and also submit the
labs in a standarized format that will streamline grading.

You will also be able to use git to commit your progress on the labs as you go.

Course git repositories will be hosted as a  repository in GitLab.  Your code will be
in a private repository that is visible only to you and course staff.

This document describes what you need to do to get started with git, and also download and upload 23500/33550 labs via GitLab.

## Contents

- [Learning Git](#learning-git)
- [Installing Git](#installing-git)
- [Setting up Git](#setting-up-git)
- [Getting Newly Released Labs](#getting-newly-released-lab)
- [Submitting Your Labs](#submitting-your-lab)
- [Word of Caution](#word-of-caution)
- [Help!](#help)


## Learning Git

There are numerous guides on using Git that are available. They range from being
interactive to just text-based. Find one that works and experiment; making
mistakes and fixing them is a great way to learn. Here is a link to resources
that GitHub suggests:
[resources for learning git](https://help.github.com/articles/what-are-other-good-resources-for-learning-git-and-github).

If you have no experience with git, you may find the following web-based tutorial helpful:
[Try Git](https://try.github.io/levels/1/challenges/1).


### Installing git <a name="installing-git"></a>
The instructions are tested on bash/linux environments. Installing git should be a simple
apt-get / yum / etc install.  

Instructions for installing git on Linux, OSX, or Windows can be found at
[GitBook: Installing](http://git-scm.com/book/en/Getting-Started-Installing-Git).

If you are using Eclipse, many versions come with git configured. The instructions will
be slightly different than the command line instructions listed but will work for any OS. 
Detailed instructions can
be found at [EGit User Guide](http://wiki.eclipse.org/EGit/User_Guide) or 
[EGit Tutorial](http://eclipsesource.com/blogs/tutorials/egit-tutorial).




## Setting Up Git <a name="setting-up-git"></a>

You should have Git installed and have beeb added to the cs235 organization already.

1. The first thing we have to do is to clone the current lab repository by
   issuing the following commands on the command line:

   ```bash
    $ git clone git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git
   ```

   This will make a complete replica of the course repository locally. Now we
   are going to change it to point to your personal repository that was created
   for you in the previous section.

   If you get an error that looks like:

   ```bash
    Cloning into 'lab'...
    Permission denied (publickey).
    fatal: Could not read from remote repository.
    ```

    Most likely the cause is that you just have not finished setting up your
    GitLab account. You just need to [setup an SSH key][ssh-key] to allow
    pushing and pulling over SSH.
    **Add your key [here](https://mit.cs.uchicago.edu/profile/keys)**

   Change your working path to your newly cloned repository:

   ```bash
    $ cd cmsc23500-win-18/
   ```

2. By default the remote called `origin` is set to the location that you cloned
   the repository from. You should see the following:

   ```bash
    $ git remote -v
        origin git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (fetch)
        origin git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (push)
   ```

   We don't want that remote to be the origin. Instead, we want to change it to
   point to your repository. To do that, issue the following command:

   ```bash
    $ git remote rename origin upstream
   ```

   And now you should see the following:

   ```bash
    $ git remote -v
        upstream git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (fetch)
        upstream git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (push)
   ```

3. Lastly we need to give your repository a new `origin` since it is lacking
   one. Issue the following command, substituting your ucnet username:

   ```bash
    $ git remote add origin git@mit.cs.uchicago.edu:cmsc23500-win-18/<ucnet-username>.git
   ```

   If you have an error that looks like the following:

   ```
  Could not rename config section 'remote.[old name]' to 'remote.[new name]'
   ```

   Or this error:
   
   ```
   fatal: remote origin already exists.
   ```
   
   This appears to happen to some depending on the version of Git they are using. To fix it,
   just issue the following command:
   
   ```bash
   $ git remote set-url origin git@mit.cs.uchicago.edu:cmsc23500-win-18/<ucnet-username>.git
   ```

   This solution was found from [StackOverflow](http://stackoverflow.com/a/2432799) thanks to
   [Cassidy Williams](https://github.com/cassidoo).

   For reference, your final `git remote -v` should look like following when it's
   setup correctly:


   ```bash
    $ git remote -v
        upstream git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (fetch)
        upstream git@mit.cs.uchicago.edu:cmsc23500-win-18/cmsc23500-win-18.git (push)
        origin git@mit.cs.uchicago.edu:cmsc23500-win-18/<ucnet-username>.git (fetch)
        origin git@mit.cs.uchicago.edu:cmsc23500-win-18/<ucnet-username>.git (push)
   ```

4. Let's test it out by doing a push of your master branch to GitLab by issuing
   the following (first make sure you are on the master branch):

   ```bash
    $ git checkout master
    $ git push origin master
   ```

   You should see something like the following:

   ```
    Counting objects: 5, done.
    Delta compression using up to 4 threads.
    Compressing objects: 100% (3/3), done.
    Writing objects: 100% (3/3), 294 bytes | 0 bytes/s, done.
    Total 3 (delta 2), reused 0 (delta 0)
    To git@mit.cs.uchicago.edu:cmsc23500-win-18/aelmore.git   f726472..545a4f0  master -> master
   ```

5. That last command was a bit special and only needs to be run the first time
   to setup the remote tracking branches. Now we should be able to just run `git
   push` without the arguments. Try it and you should get the following:

   ```bash
    $ git push
          Everything up-to-date
   ```

If you don't know Git that well, this probably seemed very arcane. Just keep
using Git and you'll understand more and more.   You aren't required to use commands like commit and push as you develop your labs, but will find them useful for debugging.  We'll provide explicit instructions on how to use these commands to actually upload your final lab solution.

## Getting Newly Released Labs and HWs <a name="getting-newly-released-lab"></a>


Pulling in labs and hws that are released or previous lab solutions should be
easy as long as you set up your repository based on the instructions in
the last section.

1. All new lab and previous lab solutions will be posted to the
   [labs](https://mit.cs.uchicago.edu/cmsc23500-win-18/cmsc23500-win-18) repository in the class
   organization.

   Check it periodically as well as Piazza's announcements for updates on
   when the new labs are released.

2. Once a lab is released, create a branch to pull in the changes.  From your simpledb directory:
   
   ```bash
        $ git checkout -b lab2
   ```
   
  Pulling in the changes should be fairly simple:

   ```bash
        $ git pull upstream master
   ```

   **OR** if you wish to be more explicit, you can `fetch` first and then
   `merge`:

   ```bash
        $ git fetch upstream
        $ git merge upstream/master
   ```
   Now commit to your new branch:
   ```bash
	$ git push origin lab2
   ```

3. If you've followed the instructions in each lab, you should have no
   merge conflicts and everything should be peachy.

## <a name="submitting-your-lab"></a> Submitting Your Labs

Instructions coming soon on submitting with chisubmit. Note that your homework
submission should exist in your master branch.


## <a name="word-of-caution"></a> Word of Caution

Git is a distributed version control system. This means everything operates
offline until you run `git pull` or `git push`. This is a great feature.

The bad thing is that you may forget to `git push` your changes. This is why we
strongly, **strongly** suggest that you check GitLab to be sure that what you
want us to see matches up with what you expect.

## <a name="help"></a> Help!

If at any point you need help with setting all this up, feel free to reach out
to one of the TAs or the instructor. 


[resources]: https://help.github.com/articles/what-are-other-good-resources-for-learning-git-and-github
[ssh-key]: https://help.github.com/articles/generating-ssh-keys


This page and infrastructure for this page was taken from https://github.com/MIT-DB-Class/course-info/blob/master/guides/course-setup.md


