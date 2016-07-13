// This file in the main entry point for defining grunt tasks and using grunt plugins.
// Click here to learn more. http://go.microsoft.com/fwlink/?LinkID=513275&clcid=0x409

module.exports = function (grunt) {
    grunt.initConfig({
    	bump: {
    		options: {
    			files: ['project.json'],
    			commit: false,
    			createTag: false,
    			push: false,
    		}
    	}
    });

    // This command registers the default task which will install bower packages into wwwroot/lib
    grunt.registerTask("default", []);

    grunt.loadNpmTasks('grunt-bump');
};