
var app = new Vue({
    el: "#app",
    data:{
        isLoaded: false
    },
    mounted: function () {
        var me = this
        setTimeout(function(){
            $("body").css('background-image','url("img/bg.jpg")');
            me.isLoaded = !me.isLoaded
        },3000)
    }
})