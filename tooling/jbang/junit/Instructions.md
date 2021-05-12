### Hibernate Reactive JUnit Test Generation

Follow these instructions to generate a working hibernate reactive junit test you can customize for your specific use-case.

---
###### SETUP

- Install and set-up [jbang](https://github.com/jbangdev/jbang)
- Development environment for [hibernate-reactive](https://github.com/hibernate/hibernate-reactive)
  * NOTE:  you can perform the **jbang** commands below by downloading the **reactive.java.qute** resource locally

---
###### STEPS
1. Open terminal and navigate to `tooling/jbang/junit` directory

2. Register the hibernate-reactive junit test template
 - `jbang template add --name reactive ./templates/reactive.java.qute`
   ```
   [someuser@localhost usertests]$ jbang template add --name reactive ./templates/reactive.java.qute
   [jbang] No explicit target pattern was set, using first file: {basename}.java=./templates/reactive.java.qute
   [jbang] Template 'reactive' added to '/home/myusername/.jbang/jbang-catalog.json'
   ```
3. Generate your unit test in the `tooling/jbang/junit/tests directory` (see [Init templates](https://github.com/jbangdev/jbang#init-templates))
    - `jbang init --template=reactive ./tests/MyUnitTest`
   ```
   [jbang] File initialized. You can now run it with 'jbang ./tests/MyUnitTest' or edit it using 'jbang edit --open=[editor] ./tests/MyUnitTest' where [editor] is your editor or IDE, e.g. 'netbeans'
   ```
4. Open your unit test in the IDEA for [editing](https://github.com/jbangdev/jbang#editing)
    - `jbang edit --open=idea ./tests/MyUnitTest.java`
    
5. Execute your unit test

